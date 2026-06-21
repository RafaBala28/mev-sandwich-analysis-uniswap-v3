"""
get_all_swaps_q1_2026.py
========================

Purpose
-------
Collect every Uniswap V3 WETH/USDC swap that occurred on Ethereum mainnet
during Q1 2026 (2026-01-01 to 2026-04-01, UTC), directly from on-chain data.
For each swap, the script gathers:
  - the decoded Swap event log (amounts, price, liquidity, tick)
  - the surrounding block metadata (timestamp, base fee, fullness, ...)
  - the transaction receipt (gas used, effective gas price, status)

This is the *base extraction* step of the data pipeline. Relay/MEV
enrichment (e.g. matching swaps to relay-submitted bundles) is intentionally
out of scope here and handled by a separate second-step script
(see enrich_relays_q1_2026.py).

The collection is organized into fixed-size block "chunks" so that long runs
can be safely interrupted and resumed: progress, failed chunks, and an audit
log are persisted to disk after every chunk.

Required inputs
----------------
- Internet access to one or more Ethereum JSON-RPC endpoints. The script
  falls back to a list of public RPC providers by default.
- Optional environment variables (none are required to run the script):
    INFURA_KEY       Infura project ID. If set, an Infura endpoint is added
                      as an additional (preferred) RPC source.
    EXTRA_RPC_URLS    Comma-separated list of additional RPC endpoint URLs
                      to use alongside the public defaults.
- Optional reference file `swaps_q1_2026_clean.csv` in the script directory,
  used only for an informational coverage diff against a previously cleaned
  dataset. The script runs fine without it.

Generated outputs (written to the script's directory)
-------------------------------------------------------
- all_swaps_q1_2026_completed_<timestamp>.csv   Final dataset once a run
                                                 finishes successfully.
- all_swaps_q1_2026_running.csv                 In-progress output file
                                                 while a run is incomplete.
- all_swaps_q1_2026_chunks/                     Per-chunk CSV files used to
                                                 assemble the final output.
- .progress_q1_2026.json                        Resume state (done/failed
                                                 chunks).
- failed_chunks_q1_2026.log                     Human-readable log of chunks
                                                 that failed to download.
- chunk_audit_q1_2026.jsonl                      Per-chunk audit trail (one
                                                 JSON record per line).
- collector_validation_q1_2026.json              Summary validation report
                                                 (duplicates, missing values,
                                                 coverage vs. reference).

Usage
-----
    python get_all_swaps_q1_2026.py                # normal run (resumable)
    python get_all_swaps_q1_2026.py --fresh-start   # discard progress, restart
    python get_all_swaps_q1_2026.py --test-mode     # quick run on a tiny block range

Required packages: pandas, requests, tqdm
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration: target pool, time window, and RPC endpoints
# ---------------------------------------------------------------------------
# Public, well-known identifiers (not secrets): the Uniswap V3 WETH/USDC
# 0.05% pool contract address, and the keccak256 topic hash of its Swap event.
POOL_ADDRESS = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# RPC credentials are read from environment variables only; no keys are
# hardcoded here. INFURA_KEY is optional and only adds one extra endpoint.
INFURA_KEY = os.getenv("INFURA_KEY", "").strip()
DEFAULT_PUBLIC_RPCS = [
    "https://ethereum.publicnode.com",
    "https://eth.llamarpc.com",
]

extra_rpcs = [u.strip() for u in os.getenv("EXTRA_RPC_URLS", "").split(",") if u.strip()]

BATCH_SIZE = 200    # max calls per JSON-RPC batch request (blocks/receipts)
CHUNK_SIZE = 2_000  # block range size used per eth_getLogs call

TS_START = 1_767_225_600   # 2026-01-01 00:00:00 UTC
TS_END = 1_775_001_600     # 2026-04-01 00:00:00 UTC

# Broad search window for timestamp-to-block resolution (binary search), wide
# enough to safely cover the Q1 2026 block range with margin.
SEARCH_BLOCK_LOWER_BOUND = 24_000_000
SEARCH_BLOCK_UPPER_BOUND = 24_900_000

# All output files are written next to this script, so the repository stays
# portable across machines (no absolute/local paths).
OUTPUT_DIR = Path(__file__).parent
RUNNING_CSV = OUTPUT_DIR / "all_swaps_q1_2026_running.csv"
FAILED_LOG = OUTPUT_DIR / "failed_chunks_q1_2026.log"
PROGRESS_FILE = OUTPUT_DIR / ".progress_q1_2026.json"
CHUNK_DIR = OUTPUT_DIR / "all_swaps_q1_2026_chunks"
CHUNK_AUDIT_LOG = OUTPUT_DIR / "chunk_audit_q1_2026.jsonl"
VALIDATION_REPORT = OUTPUT_DIR / "collector_validation_q1_2026.json"
REFERENCE_CLEAN_CSV = OUTPUT_DIR / "swaps_q1_2026_clean.csv"

CSV_FIELDS = [
    "block_number", "tx_hash", "tx_index", "log_index",
    "sender", "recipient",
    "amount0", "amount1",
    "sqrt_price_x96", "liquidity", "tick",
    "usdc_amount", "weth_amount",
    "usdc_amount_signed", "weth_amount_signed",
    "direction",
    "amountUSD", "eth_usd_price",
    "timestamp",
    "base_fee_gwei", "fee_recipient", "extra_data_text",
    "block_tx_count", "block_fullness",
    "gas_used", "effective_gas_price_gwei", "priority_fee_gwei",
    "tx_status",
    "gas_cost_usd",
]


def _unique_nonempty(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in urls:
        url = (raw or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


rpc_candidates = []
if INFURA_KEY:
    rpc_candidates.append(f"https://mainnet.infura.io/v3/{INFURA_KEY}")
rpc_candidates.extend(DEFAULT_PUBLIC_RPCS)
rpc_candidates.extend(extra_rpcs)

RPC_URLS = _unique_nonempty(rpc_candidates)
BATCH_RPCS = _unique_nonempty([
    "https://ethereum.publicnode.com",
    "https://eth.llamarpc.com",
    *extra_rpcs,
])

BATCH_RPC_DEFAULT_LIMIT = 6
BATCH_RPC_LIMITS = {
    "ethereum.publicnode.com": 6,
    "eth.llamarpc.com": 4,
}
BATCH_RPC_MIN_LIMIT = 1
BATCH_RPC_MAX_LIMITS = {
    "ethereum.publicnode.com": 10,
    "eth.llamarpc.com": 6,
}
BATCH_RPC_GROW_AFTER = 3
BATCH_RPC_GROW_STEP = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect base Uniswap V3 swap data for Q1 2026.")
    parser.add_argument("--fresh-start", "--reset", "--from-zero", action="store_true", dest="fresh_start")
    parser.add_argument("--test-mode", action="store_true", help="Run on a short 100-block range.")
    return parser.parse_args()


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_progress(data: dict) -> None:
    payload = dict(data)
    for key in ("done_chunks", "failed_chunks"):
        payload[key] = sorted(set(payload.get(key, [])))
    tmp_path = PROGRESS_FILE.with_suffix(PROGRESS_FILE.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(PROGRESS_FILE)


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def atomic_write_dataframe(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(path)


def serialize_csv_rows(rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDS, extrasaction="ignore")
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def chunk_file_path(chunk_start: int) -> Path:
    return CHUNK_DIR / f"{chunk_start}.csv"


def existing_chunk_files() -> dict[int, Path]:
    files: dict[int, Path] = {}
    if not CHUNK_DIR.exists():
        return files
    for path in CHUNK_DIR.glob("*.csv"):
        try:
            files[int(path.stem)] = path
        except ValueError:
            continue
    return files


def write_chunk_file(chunk_start: int, rows: list[dict]) -> None:
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    atomic_write_text(chunk_file_path(chunk_start), buffer.getvalue())


def append_failed_chunk(chunk_start: int, chunk_end: int, exc: Exception) -> None:
    FAILED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(FAILED_LOG, "a", encoding="utf-8") as handle:
        handle.write(
            f"{datetime.now(timezone.utc).isoformat()}\t"
            f"{chunk_start}\t{chunk_end}\t{type(exc).__name__}\t{exc}\n"
        )


def append_chunk_audit(payload: dict) -> None:
    CHUNK_AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    record = dict(payload)
    record["ts"] = datetime.now(timezone.utc).isoformat()
    with CHUNK_AUDIT_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def sync_failed_log(failed_chunks: set[int]) -> None:
    if not failed_chunks:
        try:
            FAILED_LOG.unlink(missing_ok=True)
        except OSError:
            pass
        return

    lines = [
        f"{datetime.now(timezone.utc).isoformat()}\t{chunk}\tcurrent_failed_chunk\n"
        for chunk in sorted(failed_chunks)
    ]
    atomic_write_text(FAILED_LOG, "".join(lines))


def assemble_output_csv(target_path: Path, chunk_starts: list[int]) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    seen_keys: set[tuple[int, str, int]] = set()

    with open(tmp_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()

        for chunk_start in sorted(chunk_starts):
            path = chunk_file_path(chunk_start)
            if not path.exists():
                continue
            with open(path, "r", newline="", encoding="utf-8") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    try:
                        timestamp = int(float(row["timestamp"]))
                        key = (
                            int(float(row["block_number"])),
                            row["tx_hash"],
                            int(float(row["log_index"])),
                        )
                    except (KeyError, TypeError, ValueError):
                        continue

                    if timestamp < TS_START or timestamp >= TS_END:
                        continue
                    if key in seen_keys:
                        continue

                    seen_keys.add(key)
                    writer.writerow(row)

    tmp_path.replace(target_path)


def integrity_report(df: pd.DataFrame, required_columns: list[str]) -> dict:
    report: dict = {}
    key_columns = ["block_number", "tx_hash", "log_index"]

    duplicate_mask = df.duplicated(subset=key_columns, keep=False)
    report["duplicate_rows"] = int(duplicate_mask.sum())
    report["duplicate_keys"] = int(df.loc[duplicate_mask, key_columns].drop_duplicates().shape[0])

    missing_by_column = {}
    for column in required_columns:
        if column in df.columns:
            missing_count = int(df[column].isna().sum())
            if missing_count:
                missing_by_column[column] = missing_count
        else:
            missing_by_column[column] = len(df)
    report["missing_by_column"] = missing_by_column
    return report


def new_run_csv() -> Path:
    try:
        RUNNING_CSV.unlink(missing_ok=True)
    except OSError:
        pass
    try:
        shutil.rmtree(CHUNK_DIR)
    except OSError:
        pass
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    save_progress({"done_chunks": [], "failed_chunks": [], "csv": str(RUNNING_CSV), "final_csv": ""})
    try:
        FAILED_LOG.unlink(missing_ok=True)
    except OSError:
        pass
    try:
        CHUNK_AUDIT_LOG.unlink(missing_ok=True)
    except OSError:
        pass
    return RUNNING_CSV


def get_or_init_run_csv(progress: dict) -> Path:
    csv_path = progress.get("csv")
    if csv_path:
        return Path(csv_path)
    return new_run_csv()


def latest_completed_csv() -> Path | None:
    completed = [p for p in OUTPUT_DIR.glob("all_swaps_q1_2026_completed_*.csv") if p.stat().st_size > 0]
    if not completed:
        return None
    return max(completed, key=lambda p: p.stat().st_mtime)


def finalize_csv(tmp_path: Path) -> Path:
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%Hh%M")
    final = OUTPUT_DIR / f"all_swaps_q1_2026_completed_{ts}.csv"
    tmp_path.replace(final)
    return final


def reset_run_state() -> None:
    progress = load_progress()
    csv_path = progress.get("csv")
    if csv_path:
        path = Path(csv_path)
        if path.name == RUNNING_CSV.name:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
    for path in (PROGRESS_FILE, FAILED_LOG, CHUNK_AUDIT_LOG, VALIDATION_REPORT):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    try:
        shutil.rmtree(CHUNK_DIR)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# RPC failover pool
# ---------------------------------------------------------------------------
# Public RPC endpoints are rate-limited and occasionally unreliable. This
# pool tracks a simple error count and an optional cooldown per endpoint so
# that requests are routed away from endpoints that recently failed.
class _RpcPool:
    def __init__(self, urls: list[str]):
        self.urls = urls
        self.errors = {u: 0 for u in urls}
        self.cooldown = {u: 0.0 for u in urls}
        self._idx = 0

    def ordered(self) -> list[str]:
        now = time.time()
        available = [u for u in self.urls if self.cooldown[u] <= now]
        cooling = [u for u in self.urls if self.cooldown[u] > now]
        start = self._idx % max(len(available), 1)
        rotated = available[start:] + available[:start]
        rotated.sort(key=lambda u: self.errors[u])
        return rotated + cooling

    def success(self, url: str) -> None:
        self.errors[url] = max(0, self.errors[url] - 1)
        self.cooldown[url] = 0.0
        self._idx += 1

    def failure(self, url: str, cooldown_s: float = 0.0) -> None:
        self.errors[url] += 1
        if cooldown_s:
            self.cooldown[url] = time.time() + cooldown_s


_rpc_pool = _RpcPool(RPC_URLS)
_batch_rpc_pool = _RpcPool(BATCH_RPCS)
_block_cache: dict[int, dict] = {}
_batch_rpc_state: dict[str, dict[str, int]] = {}


def _rpc_host(url: str) -> str:
    try:
        return url.split("/")[2]
    except Exception:
        return url


def _batch_limit_for_url(url: str) -> int:
    host = _rpc_host(url)
    state = _batch_rpc_state.get(host)
    if state is not None:
        return state["limit"]
    return BATCH_RPC_LIMITS.get(host, BATCH_RPC_DEFAULT_LIMIT)


def _batch_limit_ceiling_for_url(url: str) -> int:
    return BATCH_RPC_MAX_LIMITS.get(_rpc_host(url), BATCH_RPC_DEFAULT_LIMIT)


def _batch_state_for_url(url: str) -> dict[str, int]:
    host = _rpc_host(url)
    if host not in _batch_rpc_state:
        _batch_rpc_state[host] = {
            "limit": BATCH_RPC_LIMITS.get(host, BATCH_RPC_DEFAULT_LIMIT),
            "streak": 0,
        }
    return _batch_rpc_state[host]


# Adaptive batch sizing: each RPC host starts with a conservative batch
# limit and is allowed to grow gradually after consecutive successes, or
# shrink immediately after a failure. This keeps batch requests within
# whatever limit each public endpoint happens to enforce.
def _batch_record_success(url: str) -> None:
    state = _batch_state_for_url(url)
    state["streak"] += 1
    if state["streak"] >= BATCH_RPC_GROW_AFTER:
        ceiling = _batch_limit_ceiling_for_url(url)
        if state["limit"] < ceiling:
            state["limit"] = min(ceiling, state["limit"] + BATCH_RPC_GROW_STEP)
        state["streak"] = 0


def _batch_record_failure(url: str) -> None:
    state = _batch_state_for_url(url)
    state["limit"] = max(BATCH_RPC_MIN_LIMIT, max(1, state["limit"] // 2))
    state["streak"] = 0


def _batch_error_indicates_too_large(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "too many rpc calls in batch request" in text
        or "batch list" in text
        or "missing receipt result" in text
        or "batch id mismatch" in text
    )


def _post_batch_for_url(url: str, calls: list[dict]) -> list[dict]:
    headers = {"Content-Type": "application/json"}
    if not calls:
        return []

    limit = max(1, _batch_limit_for_url(url))
    if len(calls) > limit:
        left = _post_batch_for_url(url, calls[:limit])
        right = _post_batch_for_url(url, calls[limit:])
        return left + right

    try:
        response = requests.post(url, json=calls, headers=headers, timeout=60)
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 5))
            tqdm.write(f"  Rate limit {_rpc_host(url)} - cooling {wait}s")
            _batch_rpc_pool.failure(url, cooldown_s=wait)
            _batch_record_failure(url)
            time.sleep(wait)
            raise RuntimeError(f"rate_limited:{wait}")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Expected batch list, got: {str(data)[:120]}")
        _batch_record_success(url)
        return data
    except Exception as exc:
        if len(calls) > 1 and _batch_error_indicates_too_large(exc):
            _batch_record_failure(url)
            midpoint = max(1, len(calls) // 2)
            return _post_batch_for_url(url, calls[:midpoint]) + _post_batch_for_url(url, calls[midpoint:])
        _batch_record_failure(url)
        raise


def rpc(method: str, params: list, retries: int = 4):
    headers = {"Content-Type": "application/json"}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    urls = _rpc_pool.ordered()
    for attempt, url in enumerate(urls[:retries], 1):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", min(2 ** attempt, 30)))
                tqdm.write(f"  Rate limit {_rpc_host(url)} - cooling {wait}s (attempt {attempt}/{retries})")
                _rpc_pool.failure(url, cooldown_s=wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
            body = response.json()
            if "error" in body:
                raise ValueError(body["error"])
            _rpc_pool.success(url)
            return body["result"]
        except Exception as exc:
            tqdm.write(f"  RPC attempt {attempt}/{retries} on {_rpc_host(url)}: {exc}")
            _rpc_pool.failure(url)
            if attempt == retries:
                raise
            time.sleep(min(2 ** attempt, 16))


def batch_rpc(calls: list[dict], retries: int = 6) -> list:
    urls = _batch_rpc_pool.ordered()
    for attempt, url in enumerate(urls[:retries], 1):
        try:
            data = _post_batch_for_url(url, calls)
            if not isinstance(data, list):
                raise ValueError(f"Expected batch list, got: {str(data)[:120]}")
            _batch_rpc_pool.success(url)
            return data
        except Exception as exc:
            tqdm.write(f"  batch_rpc attempt {attempt}/{retries} on {_rpc_host(url)}: {exc}")
            _batch_rpc_pool.failure(url)
            if attempt == retries:
                raise
            time.sleep(min(2 ** attempt, 30))


def _query_logs_once(url: str, chunk_start: int, chunk_end: int) -> list[dict]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(chunk_start),
            "toBlock": hex(chunk_end),
            "address": POOL_ADDRESS,
            "topics": [SWAP_TOPIC],
        }],
    }
    response = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=45)
    if response.status_code == 429:
        wait = int(response.headers.get("Retry-After", 5))
        _rpc_pool.failure(url, cooldown_s=wait)
        raise RuntimeError(f"rate_limited:{wait}")
    response.raise_for_status()
    body = response.json()
    if "error" in body:
        raise ValueError(body["error"])
    result = body.get("result")
    if result is None:
        raise ValueError("eth_getLogs returned null result")
    if not isinstance(result, list):
        raise TypeError(f"eth_getLogs returned {type(result).__name__} instead of list")
    _rpc_pool.success(url)
    return result


def fetch_logs_verified(chunk_start: int, chunk_end: int, active_period: bool) -> tuple[list[dict], dict]:
    confirmations_required = 2 if active_period else 1
    candidate_urls = _rpc_pool.ordered()
    attempts: list[dict] = []
    empty_confirmations: list[str] = []
    max_rounds = 2

    for round_idx in range(1, max_rounds + 1):
        for url in candidate_urls[: min(len(candidate_urls), 4)]:
            try:
                logs = _query_logs_once(url, chunk_start, chunk_end)
                attempts.append({
                    "rpc": _rpc_host(url),
                    "round": round_idx,
                    "status": "ok",
                    "logs": len(logs),
                })
                if logs:
                    return logs, {
                        "status": "success",
                        "attempts": attempts,
                        "empty_confirmations": empty_confirmations,
                    }
                if _rpc_host(url) not in empty_confirmations:
                    empty_confirmations.append(_rpc_host(url))
                if len(empty_confirmations) >= confirmations_required:
                    return [], {
                        "status": "empty_verified",
                        "attempts": attempts,
                        "empty_confirmations": empty_confirmations,
                    }
            except Exception as exc:
                attempts.append({
                    "rpc": _rpc_host(url),
                    "round": round_idx,
                    "status": "error",
                    "error": str(exc)[:300],
                })
                _rpc_pool.failure(url)
                time.sleep(min(2 ** round_idx, 8))

    raise RuntimeError(
        "Unable to verify chunk logs reliably; "
        f"empty_confirmations={empty_confirmations}, attempts={attempts[:6]}"
    )


def batch_rpc_strict(calls: list[dict], retries: int = 6) -> list[dict]:
    expected_ids = [call["id"] for call in calls]
    expected_id_set = set(expected_ids)
    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            data = batch_rpc(calls, retries=1)
            if len(data) != len(calls):
                raise ValueError(f"Expected {len(calls)} responses, got {len(data)}")

            seen_ids: set[int] = set()
            by_id: dict[int, dict] = {}
            for item in data:
                if "id" not in item:
                    raise ValueError("Batch response missing id")
                if item["id"] in seen_ids:
                    raise ValueError(f"Duplicate batch response id: {item['id']}")
                if "error" in item:
                    raise ValueError(f"Batch response error for id {item['id']}: {item['error']}")
                seen_ids.add(item["id"])
                by_id[item["id"]] = item

            if seen_ids != expected_id_set:
                missing = sorted(expected_id_set - seen_ids)
                extra = sorted(seen_ids - expected_id_set)
                raise ValueError(f"Batch id mismatch. Missing={missing[:5]} Extra={extra[:5]}")

            return [by_id[i] for i in expected_ids]
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                raise
            time.sleep(min(2 ** attempt, 30))

    assert last_exc is not None
    raise last_exc


def _parse_block(result: dict) -> dict:
    gas_used = int(result.get("gasUsed", "0x0"), 16)
    gas_limit = int(result.get("gasLimit", "0x1"), 16)
    raw_extra = result.get("extraData", "0x")[2:]
    return {
        "timestamp": int(result.get("timestamp", "0x0"), 16),
        "base_fee_gwei": int(result.get("baseFeePerGas", "0x0"), 16) / 1e9,
        "fee_recipient": result.get("miner") or result.get("feeRecipient") or "",
        "block_tx_count": len(result.get("transactions", [])),
        "block_fullness": round(gas_used / gas_limit, 4) if gas_limit else 0,
        "extra_data_text": bytes.fromhex(raw_extra).decode("utf-8", errors="ignore").strip(),
    }


def ensure_block(block_num: int) -> dict:
    if block_num not in _block_cache:
        result = rpc("eth_getBlockByNumber", [hex(block_num), False])
        if result is None:
            raise ValueError(f"Block {block_num} not found")
        _block_cache[block_num] = _parse_block(result)
    return _block_cache[block_num]


# Standard binary search over block numbers to find the first block whose
# timestamp is >= the target timestamp. Used to convert the Q1 2026 date
# window into a concrete start/end block range.
def find_first_block_at_or_after(timestamp: int, low: int, high: int) -> int:
    result = high
    while low <= high:
        mid = (low + high) // 2
        block = ensure_block(mid)
        block_ts = int(block.get("timestamp") or 0)
        if block_ts >= timestamp:
            result = mid
            high = mid - 1
        else:
            low = mid + 1
    return result


def resolve_block_range(test_mode: bool) -> tuple[int, int]:
    if test_mode:
        return 24_200_000, 24_200_100

    block_start = find_first_block_at_or_after(
        TS_START,
        SEARCH_BLOCK_LOWER_BOUND,
        SEARCH_BLOCK_UPPER_BOUND,
    )
    block_end_exclusive = find_first_block_at_or_after(
        TS_END,
        block_start,
        SEARCH_BLOCK_UPPER_BOUND,
    )
    return block_start, max(block_start, block_end_exclusive - 1)


def prefetch_blocks(block_nums: list[int]) -> None:
    missing = [b for b in block_nums if b not in _block_cache]
    if not missing:
        return
    for i in range(0, len(missing), BATCH_SIZE):
        batch_nums = missing[i : i + BATCH_SIZE]
        calls = [
            {"jsonrpc": "2.0", "id": j, "method": "eth_getBlockByNumber", "params": [hex(n), False]}
            for j, n in enumerate(batch_nums)
        ]
        responses = batch_rpc_strict(calls)
        id_to_num = {j: n for j, n in enumerate(batch_nums)}
        for item in responses:
            result = item.get("result")
            num = id_to_num.get(item.get("id"))
            if num is None:
                continue
            if not result:
                raise ValueError(f"Missing block result for id {item.get('id')} / block {num}")
            _block_cache[num] = _parse_block(result)


def fetch_receipts(tx_hashes: list[str]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for i in range(0, len(tx_hashes), BATCH_SIZE):
        batch = tx_hashes[i : i + BATCH_SIZE]
        calls = [
            {"jsonrpc": "2.0", "id": j, "method": "eth_getTransactionReceipt", "params": [h]}
            for j, h in enumerate(batch)
        ]
        for item in batch_rpc_strict(calls):
            receipt = item.get("result")
            if not receipt:
                raise ValueError(f"Missing receipt result for id {item.get('id')}")
            if "gasUsed" not in receipt or "effectiveGasPrice" not in receipt:
                raise ValueError(f"Incomplete receipt payload for tx {receipt.get('transactionHash', '')}")
            results[receipt["transactionHash"]] = {
                "gas_used": int(receipt["gasUsed"], 16),
                "effective_gas_price_gwei": int(receipt["effectiveGasPrice"], 16) / 1e9,
                "tx_status": int(receipt.get("status", "0x1"), 16),
            }
    return results


def to_int256(hex32: str) -> int:
    value = int(hex32, 16)
    return value - 2**256 if value >= 2**255 else value


def to_uint(hex32: str) -> int:
    return int(hex32, 16)


def compute_eth_usd_from_sqrt_price(sqrt_price_x96: int) -> float:
    if sqrt_price_x96 <= 0:
        return 0.0
    raw_ratio = (sqrt_price_x96 / 2**96) ** 2
    if raw_ratio <= 0:
        return 0.0
    # Uniswap V3 price is token1/token0 in raw units. For token0=USDC (6)
    # and token1=WETH (18), human WETH-per-USDC is raw_ratio * 1e-12.
    # We want USD-per-WETH, so invert it.
    return 1e12 / raw_ratio


def compute_usd(amount0: int, amount1: int, sqrt_price_x96: int) -> float:
    if amount0 != 0:
        return abs(amount0) / 1e6
    eth_usd = compute_eth_usd_from_sqrt_price(sqrt_price_x96)
    if eth_usd <= 0:
        return 0.0
    return abs(amount1) / 1e18 * eth_usd


def decode_log(log: dict) -> dict | None:
    """Decode a raw Uniswap V3 Swap event log into its typed fields.

    Returns None (instead of raising) for any malformed log, so a single
    bad log does not abort the whole chunk.
    """
    try:
        topics = log["topics"]
        data = log["data"][2:]
        # topics[1]/topics[2] are the indexed `sender`/`recipient` addresses,
        # padded to 32 bytes; the address itself is the last 20 bytes (40 hex chars).
        sender = "0x" + topics[1][-40:]
        recipient = "0x" + topics[2][-40:]
        amount0 = to_int256(data[0:64])
        amount1 = to_int256(data[64:128])
        sqrt_price_x96 = to_uint(data[128:192])
        liquidity = to_uint(data[192:256])
        tick = to_int256(data[256:320])
        return {
            "block_number": int(log["blockNumber"], 16),
            "tx_hash": log["transactionHash"],
            "tx_index": int(log["transactionIndex"], 16),
            "log_index": int(log["logIndex"], 16),
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
            "amountUSD": f"{compute_usd(amount0, amount1, sqrt_price_x96):.6f}",
        }
    except Exception:
        return None


def enrich_row(row: dict, receipt: dict) -> dict:
    """Add human-readable amounts, USD pricing, and gas/fee fields to a decoded swap row."""
    amount0 = row["amount0"]
    amount1 = row["amount1"]

    row["usdc_amount_signed"] = amount0 / 1e6
    row["weth_amount_signed"] = amount1 / 1e18
    row["usdc_amount"] = abs(row["usdc_amount_signed"])
    row["weth_amount"] = abs(row["weth_amount_signed"])
    row["direction"] = "BUY_WETH" if amount0 > 0 else "SELL_WETH"

    if amount0 != 0 and amount1 != 0:
        eth_usd = abs(amount0 / 1e6) / abs(amount1 / 1e18)
        row["eth_usd_price"] = f"{eth_usd:.2f}"
    else:
        eth_usd = compute_eth_usd_from_sqrt_price(int(row.get("sqrt_price_x96", 0) or 0))
        row["eth_usd_price"] = f"{eth_usd:.2f}" if eth_usd > 0 else ""

    block = _block_cache.get(row["block_number"], {})
    row["timestamp"] = block.get("timestamp", "")
    row["base_fee_gwei"] = f"{block.get('base_fee_gwei', 0):.4f}"
    row["fee_recipient"] = block.get("fee_recipient", "")
    row["extra_data_text"] = block.get("extra_data_text", "")
    row["block_tx_count"] = block.get("block_tx_count", "")
    row["block_fullness"] = block.get("block_fullness", "")

    gas_used = receipt.get("gas_used", 0)
    eff_gas_gwei = receipt.get("effective_gas_price_gwei", 0.0)
    base_fee = block.get("base_fee_gwei", 0)
    row["gas_used"] = gas_used
    row["effective_gas_price_gwei"] = f"{eff_gas_gwei:.4f}"
    row["priority_fee_gwei"] = round(eff_gas_gwei - base_fee, 4) if eff_gas_gwei and base_fee else ""
    row["tx_status"] = receipt.get("tx_status", "")
    row["gas_cost_usd"] = f"{gas_used * eff_gas_gwei * 1e-9 * eth_usd:.6f}" if gas_used and eth_usd else ""
    return row


def print_dataset_preview(df: pd.DataFrame) -> None:
    cols_preview = [
        "block_number", "tx_hash", "direction", "eth_usd_price",
        "base_fee_gwei", "block_fullness", "tx_status", "gas_cost_usd",
    ]
    print(df[cols_preview].head(3).to_string(index=False))


def load_chunk_audit_latest() -> dict[int, dict]:
    latest: dict[int, dict] = {}
    if not CHUNK_AUDIT_LOG.exists():
        return latest
    with CHUNK_AUDIT_LOG.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                chunk_start = int(record["chunk_start"])
            except Exception:
                continue
            latest[chunk_start] = record
    return latest


def read_chunk_row_counts(chunk_starts: list[int]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for chunk_start in chunk_starts:
        path = chunk_file_path(chunk_start)
        count = 0
        if path.exists():
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if (row.get("tx_hash") or "").strip():
                        count += 1
        counts[chunk_start] = count
    return counts


# Heuristic QA pass: flags chunks whose row count looks anomalous compared
# to their neighbors (e.g. a chunk with 0 swaps sandwiched between two busy
# chunks), so they can be re-fetched and double-checked before finalizing.
def find_suspicious_chunks(chunk_starts: list[int]) -> list[int]:
    ordered = sorted(chunk_starts)
    counts = read_chunk_row_counts(ordered)
    audits = load_chunk_audit_latest()
    suspicious: list[int] = []
    seen: set[int] = set()
    for idx in range(1, len(ordered) - 1):
        prev_chunk = ordered[idx - 1]
        curr_chunk = ordered[idx]
        next_chunk = ordered[idx + 1]
        prev_count = counts.get(prev_chunk, 0)
        curr_count = counts.get(curr_chunk, 0)
        next_count = counts.get(next_chunk, 0)
        if curr_count == 0 and prev_count >= 100 and next_count >= 100:
            if curr_chunk not in seen:
                suspicious.append(curr_chunk)
                seen.add(curr_chunk)
            continue
        if prev_count >= 300 and next_count >= 300 and curr_count * 20 < min(prev_count, next_count):
            if curr_chunk not in seen:
                suspicious.append(curr_chunk)
                seen.add(curr_chunk)
    for chunk_start in ordered:
        count = counts.get(chunk_start, 0)
        audit = audits.get(chunk_start)
        if count == 0 and (not audit or audit.get("status") != "empty_verified"):
            if chunk_start not in seen:
                suspicious.append(chunk_start)
                seen.add(chunk_start)
        if audit and audit.get("status") == "failed":
            if chunk_start not in seen:
                suspicious.append(chunk_start)
                seen.add(chunk_start)
    return suspicious


# Optional sanity check: if a previously cleaned reference dataset exists
# (swaps_q1_2026_clean.csv), compare its swap keys against this run's output
# over the overlapping block range, purely for informational coverage stats.
def build_reference_diff_report(output_path: Path) -> dict:
    report = {
        "reference_path": str(REFERENCE_CLEAN_CSV),
        "reference_exists": REFERENCE_CLEAN_CSV.exists(),
    }
    if not REFERENCE_CLEAN_CSV.exists() or not output_path.exists():
        return report

    output_df = pd.read_csv(output_path, usecols=["block_number", "tx_hash", "log_index"], low_memory=False)
    ref_df = pd.read_csv(REFERENCE_CLEAN_CSV, usecols=["block_number", "tx_hash", "log_index"], low_memory=False)

    output_df = output_df.dropna(subset=["block_number", "tx_hash", "log_index"])
    ref_df = ref_df.dropna(subset=["block_number", "tx_hash", "log_index"])

    output_df["block_number"] = pd.to_numeric(output_df["block_number"], errors="raise").astype("int64")
    output_df["log_index"] = pd.to_numeric(output_df["log_index"], errors="raise").astype("int64")
    ref_df["block_number"] = pd.to_numeric(ref_df["block_number"], errors="raise").astype("int64")
    ref_df["log_index"] = pd.to_numeric(ref_df["log_index"], errors="raise").astype("int64")

    block_min = int(output_df["block_number"].min()) if len(output_df) else None
    block_max = int(output_df["block_number"].max()) if len(output_df) else None
    report["output_block_min"] = block_min
    report["output_block_max"] = block_max
    if block_min is None or block_max is None:
        return report

    ref_slice = ref_df[(ref_df["block_number"] >= block_min) & (ref_df["block_number"] <= block_max)]
    output_keys = set(zip(output_df["block_number"], output_df["tx_hash"], output_df["log_index"]))
    ref_keys = set(zip(ref_slice["block_number"], ref_slice["tx_hash"], ref_slice["log_index"]))
    common = output_keys & ref_keys
    missing = ref_keys - output_keys
    extra = output_keys - ref_keys

    report.update({
        "reference_rows_in_range": len(ref_slice),
        "output_rows": len(output_df),
        "common_keys": len(common),
        "missing_vs_reference": len(missing),
        "extra_vs_reference": len(extra),
        "coverage_vs_reference_pct": round((len(common) / len(ref_keys) * 100), 4) if ref_keys else None,
        "sample_missing": list(sorted(missing))[:10],
        "sample_extra": list(sorted(extra))[:10],
    })
    return report


def build_validation_report(
    output_path: Path,
    done_chunks: set[int],
    failed_chunks: set[int],
    suspicious_chunks: list[int],
) -> dict:
    chunk_counts = read_chunk_row_counts(sorted(done_chunks))
    audit_latest = load_chunk_audit_latest()
    empty_verified = sum(
        1
        for chunk_start, count in chunk_counts.items()
        if count == 0 and audit_latest.get(chunk_start, {}).get("status") == "empty_verified"
    )
    unverified_empty = [
        chunk_start
        for chunk_start, count in chunk_counts.items()
        if count == 0 and audit_latest.get(chunk_start, {}).get("status") != "empty_verified"
    ]

    report = {
        "done_chunks": len(done_chunks),
        "failed_chunks": sorted(failed_chunks),
        "suspicious_chunks": sorted(suspicious_chunks),
        "data_rows_in_chunks": int(sum(chunk_counts.values())),
        "empty_verified_chunks": int(empty_verified),
        "unverified_empty_chunks": unverified_empty,
        "audit_records": len(audit_latest),
        "reference_diff": build_reference_diff_report(output_path),
    }
    return report


# Process a single block-range chunk end-to-end: fetch + verify logs, decode
# swaps, attach block/receipt data, and persist the chunk to its own CSV file.
# Each chunk is self-contained so the overall run can be resumed safely after
# an interruption (only unfinished chunks are reprocessed).
def process_chunk(
    chunk_start: int,
    chunk_end: int,
    done_chunks: set[int],
    failed_set: set[int],
    progress: dict,
) -> int:
    ts = ensure_block(chunk_start)["timestamp"]
    active_period = TS_START <= ts < TS_END
    if ts >= TS_END + 86_400 or ts < TS_START - 86_400:
        write_chunk_file(chunk_start, [])
        done_chunks.add(chunk_start)
        failed_set.discard(chunk_start)
        save_progress({**progress, "done_chunks": list(done_chunks), "failed_chunks": list(failed_set)})
        sync_failed_log(failed_set)
        append_chunk_audit({
            "chunk_start": chunk_start,
            "chunk_end": chunk_end,
            "status": "skipped_outside_window",
            "logs_returned": 0,
            "retry_count": 0,
            "active_period": active_period,
        })
        return 0

    logs, log_meta = fetch_logs_verified(chunk_start, chunk_end, active_period=active_period)

    raw_rows = []
    block_nums: list[int] = []
    seen_blocks: set[int] = set()
    tx_hashes: list[str] = []
    seen_tx: set[str] = set()

    for log in logs:
        row = decode_log(log)
        if not row:
            continue
        raw_rows.append(row)
        block_num = row["block_number"]
        if block_num not in seen_blocks:
            block_nums.append(block_num)
            seen_blocks.add(block_num)
        tx_hash = row["tx_hash"]
        if tx_hash not in seen_tx:
            tx_hashes.append(tx_hash)
            seen_tx.add(tx_hash)

    if not raw_rows:
        if active_period and log_meta["status"] != "empty_verified":
            raise RuntimeError(f"Unverified empty raw_rows for active chunk {chunk_start}-{chunk_end}")
        write_chunk_file(chunk_start, [])
        done_chunks.add(chunk_start)
        failed_set.discard(chunk_start)
        save_progress({**progress, "done_chunks": list(done_chunks), "failed_chunks": list(failed_set)})
        sync_failed_log(failed_set)
        append_chunk_audit({
            "chunk_start": chunk_start,
            "chunk_end": chunk_end,
            "status": "empty_verified",
            "logs_returned": 0,
            "retry_count": max(len(log_meta.get("attempts", [])) - 1, 0),
            "active_period": active_period,
            "attempts": log_meta.get("attempts", []),
        })
        return 0

    prefetch_blocks(block_nums)
    receipts = fetch_receipts(tx_hashes)

    chunk_rows: list[dict] = []
    for row in raw_rows:
        receipt = receipts.get(row["tx_hash"], {})
        enriched = enrich_row(row, receipt)
        ts_row = enriched.get("timestamp") or 0
        if ts_row and (ts_row < TS_START or ts_row >= TS_END):
            continue
        chunk_rows.append(enriched)

    write_chunk_file(chunk_start, chunk_rows)
    done_chunks.add(chunk_start)
    failed_set.discard(chunk_start)
    save_progress({**progress, "done_chunks": list(done_chunks), "failed_chunks": list(failed_set)})
    sync_failed_log(failed_set)
    append_chunk_audit({
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "status": "success",
        "logs_returned": len(raw_rows),
        "rows_written": len(chunk_rows),
        "retry_count": max(len(log_meta.get("attempts", [])) - 1, 0),
        "active_period": active_period,
        "attempts": log_meta.get("attempts", []),
    })
    return len(chunk_rows)


def main() -> None:
    """Entry point. Runs three passes over the Q1 2026 block range:
    1) process all pending chunks, 2) retry any chunks that failed,
    3) re-check any chunks flagged as suspicious by the row-count heuristic.
    The final CSV is only marked complete if all three passes leave no
    failed, suspicious, or unverified-empty chunks behind.
    """
    args = parse_args()
    if args.fresh_start:
        reset_run_state()

    progress = load_progress()
    block_start, block_end = resolve_block_range(args.test_mode)
    all_chunks = list(range(block_start, block_end + 1, CHUNK_SIZE if not args.test_mode else 100))

    done_chunks = set(progress.get("done_chunks", []))
    failed_set = set(progress.get("failed_chunks", []))
    final_csv = progress.get("final_csv")
    if final_csv and Path(final_csv).exists() and len(done_chunks) == len(all_chunks) and not failed_set:
        df = pd.read_csv(final_csv, low_memory=False)
        print(f"Output : {Path(final_csv).name}")
        print(f"Block range   : {block_start:,} - {block_end:,}")
        print(f"Chunks        : {len(all_chunks):,}  ({CHUNK_SIZE if not args.test_mode else 100} blocks each)")
        print("Run already complete. Reusing existing final CSV.")
        print(f"\nTotal swaps   : {len(df):,}")
        if len(df):
            print(f"Block range   : {df['block_number'].min():,} - {df['block_number'].max():,}")
        print_dataset_preview(df)
        return

    output_csv = get_or_init_run_csv(progress)
    chunk_files = existing_chunk_files()
    done_chunks = set(chunk_files) | {chunk for chunk in done_chunks if chunk_file_path(chunk).exists()}
    failed_set = {chunk for chunk in failed_set if chunk not in done_chunks}
    progress["csv"] = str(output_csv)
    progress["done_chunks"] = list(done_chunks)
    progress["failed_chunks"] = list(failed_set)
    progress.setdefault("final_csv", "")
    save_progress(progress)

    pending = [chunk for chunk in all_chunks if chunk not in done_chunks]
    queue = pending

    print(f"{'[TEST MODE] ' if args.test_mode else ''}Output : {output_csv.name}")
    if args.fresh_start:
        print("Fresh start  : enabled")
    print(f"Block range   : {block_start:,} - {block_end:,}")
    print(f"Chunks        : {len(all_chunks):,}  ({CHUNK_SIZE if not args.test_mode else 100} blocks each)")
    print(f"Resume done   : {len(done_chunks):,}")
    print(f"Retry queued  : {len(queue) - len(pending):,}")
    print(f"RPC (logs)    : {RPC_URLS[0]}  (with failover across {len(RPC_URLS)} endpoints)")
    print(f"RPC (batch)   : {BATCH_RPCS[0]}  (with failover across {len(BATCH_RPCS)} endpoints)\n")

    rows_written = 0
    failed_chunks = 0
    pbar = tqdm(queue, desc="Chunks", unit="chunk")

    def update_progress_postfix() -> None:
        pbar.set_postfix(swaps=rows_written, failed=failed_chunks, refresh=False)

    update_progress_postfix()
    for chunk_start in pbar:
        chunk_end = min(chunk_start + (CHUNK_SIZE if not args.test_mode else 100) - 1, block_end)
        try:
            rows_written += process_chunk(chunk_start, chunk_end, done_chunks, failed_set, progress)

        except Exception as exc:
            tqdm.write(f"  FAILED chunk {chunk_start}-{chunk_end}: {exc}")
            append_failed_chunk(chunk_start, chunk_end, exc)
            append_chunk_audit({
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
                "status": "failed",
                "logs_returned": None,
                "retry_count": None,
                "active_period": True,
                "error": str(exc)[:500],
            })
            failed_chunks += 1
            failed_set.add(chunk_start)
            save_progress({**progress, "done_chunks": list(done_chunks), "failed_chunks": list(failed_set)})
            sync_failed_log(failed_set)
            update_progress_postfix()
            time.sleep(2)
            continue

        time.sleep(0.1)
        update_progress_postfix()

    final_retry_targets = sorted(failed_set)
    if final_retry_targets:
        tqdm.write(f"\nFinal retry pass for {len(final_retry_targets):,} failed chunk(s).")
        for chunk_start in final_retry_targets:
            chunk_end = min(chunk_start + (CHUNK_SIZE if not args.test_mode else 100) - 1, block_end)
            try:
                rows_written += process_chunk(chunk_start, chunk_end, done_chunks, failed_set, progress)
            except Exception as exc:
                tqdm.write(f"  RETRY FAILED chunk {chunk_start}-{chunk_end}: {exc}")
                append_failed_chunk(chunk_start, chunk_end, exc)
                append_chunk_audit({
                    "chunk_start": chunk_start,
                    "chunk_end": chunk_end,
                    "status": "failed",
                    "logs_returned": None,
                    "retry_count": None,
                    "active_period": True,
                    "error": str(exc)[:500],
                })
                failed_set.add(chunk_start)
                save_progress({**progress, "done_chunks": list(done_chunks), "failed_chunks": list(failed_set)})
                sync_failed_log(failed_set)
                time.sleep(2)

    suspicious_chunks = [chunk for chunk in find_suspicious_chunks(list(done_chunks)) if chunk not in failed_set]
    if suspicious_chunks:
        tqdm.write(f"\nSuspicious chunk recheck pass for {len(suspicious_chunks):,} chunk(s).")
        for chunk_start in suspicious_chunks:
            chunk_end = min(chunk_start + (CHUNK_SIZE if not args.test_mode else 100) - 1, block_end)
            try:
                rows_written += process_chunk(chunk_start, chunk_end, done_chunks, failed_set, progress)
            except Exception as exc:
                tqdm.write(f"  SUSPICIOUS RECHECK FAILED chunk {chunk_start}-{chunk_end}: {exc}")
                append_failed_chunk(chunk_start, chunk_end, exc)
                append_chunk_audit({
                    "chunk_start": chunk_start,
                    "chunk_end": chunk_end,
                    "status": "failed",
                    "logs_returned": None,
                    "retry_count": None,
                    "active_period": True,
                    "error": str(exc)[:500],
                })
                failed_set.add(chunk_start)
                save_progress({**progress, "done_chunks": list(done_chunks), "failed_chunks": list(failed_set)})
                sync_failed_log(failed_set)
                time.sleep(2)

    sync_failed_log(failed_set)

    progress["done_chunks"] = list(done_chunks)
    progress["failed_chunks"] = list(failed_set)
    progress["csv"] = str(output_csv)
    save_progress(progress)

    assemble_output_csv(output_csv, list(done_chunks))
    df = pd.read_csv(output_csv, low_memory=False)
    final_suspicious_chunks = find_suspicious_chunks(list(done_chunks))
    validation_report = build_validation_report(output_csv, done_chunks, failed_set, final_suspicious_chunks)
    atomic_write_text(VALIDATION_REPORT, json.dumps(validation_report, indent=2))

    required_columns = [
        "block_number", "tx_hash", "tx_index", "log_index",
        "sender", "recipient", "amount0", "amount1",
        "sqrt_price_x96", "liquidity", "tick",
        "usdc_amount", "weth_amount", "usdc_amount_signed", "weth_amount_signed",
        "direction", "amountUSD", "eth_usd_price", "timestamp",
        "base_fee_gwei", "fee_recipient", "extra_data_text",
        "block_tx_count", "block_fullness", "gas_used",
        "effective_gas_price_gwei", "priority_fee_gwei", "tx_status",
        "gas_cost_usd",
    ]
    integrity = integrity_report(df, required_columns)

    is_complete = (
        len(done_chunks) == len(all_chunks)
        and not failed_set
        and not final_suspicious_chunks
        and not validation_report["unverified_empty_chunks"]
    )
    final_output = output_csv
    if not args.test_mode and is_complete:
        final_output = finalize_csv(output_csv)
        progress["csv"] = str(final_output)
        progress["final_csv"] = str(final_output)
        save_progress(progress)
    else:
        progress["csv"] = str(output_csv)
        save_progress(progress)

    print(f"\nTotal swaps   : {len(df):,}")
    if len(df):
        print(f"Block range   : {df['block_number'].min():,} - {df['block_number'].max():,}")
        ts_min = df["timestamp"].min()
        ts_max = df["timestamp"].max()
        if ts_min and ts_max:
            print(
                "Date range    : "
                f"{datetime.fromtimestamp(ts_min, tz=timezone.utc).date()} - "
                f"{datetime.fromtimestamp(ts_max, tz=timezone.utc).date()}"
            )
    print(f"Output        : {final_output.name}")
    print(f"Failed chunks : {failed_chunks}")
    print(f"Suspicious chunks remaining : {len(final_suspicious_chunks)}")
    print(f"Unverified empty chunks     : {len(validation_report['unverified_empty_chunks'])}")
    print(f"Duplicate rows : {integrity['duplicate_rows']}")
    print(f"Duplicate keys : {integrity['duplicate_keys']}")
    ref_diff = validation_report["reference_diff"]
    if ref_diff.get("reference_exists"):
        print(f"Reference coverage : {ref_diff.get('coverage_vs_reference_pct')}%")
        print(f"Reference missing  : {ref_diff.get('missing_vs_reference')}")
        print(f"Reference extra    : {ref_diff.get('extra_vs_reference')}")
    if integrity["missing_by_column"]:
        print("Missing values (required columns):")
        for column, missing_count in sorted(integrity["missing_by_column"].items()):
            print(f"  {column:<30} {missing_count:,}")
    else:
        print("Missing values (required columns): none")
    if not is_complete and not args.test_mode:
        print(f"Run incomplete: {len(all_chunks) - len(done_chunks):,} chunk(s) still pending.")
        print("Progress file preserved. Re-run script to continue and resolve failed/suspicious chunks.")

    print()
    print_dataset_preview(df)


if __name__ == "__main__":
    main()
