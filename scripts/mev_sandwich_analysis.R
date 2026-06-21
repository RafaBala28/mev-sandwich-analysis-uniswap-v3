# ============================================================================
# An Economic Analysis of MEV Extraction on Uniswap V3:
# Sandwich Attacks, Bot Profitability, and Victim Losses
#
# Uniswap V3 WETH/USDC 0.05% pool, Q1 2026
#
# This script reproduces all tables and figures from the seminar paper.
# It detects BUY-side and SELL-side sandwich attacks, classifies them by
# backrun type, computes gross/net profit, decomposes profitability into
# AMM profit / slippage / pool fees / gas, measures victim damage and
# slippage, and analyses attacker and builder concentration.
#
# Headline results reproduced:
#   - 699 detected sandwich attacks (363 buy-side, 336 sell-side)
#   - 59 unique attacker addresses, top 10 = 81.3% (Gini = 0.79)
#   - Aggregate net loss of USD -52'724 (only full closes profitable)
#   - Total victim execution loss USD 323'112 (59.9% sandwich, 40.1% slippage)
#   - Titan dominant builder (59.4% of attacks)
# ============================================================================


# ============================================================================
# 0. SETUP
# ============================================================================

library(tidyverse)
library(lubridate)
library(scales)
library(zoo)
library(ineq)

# Optional (only needed to export LaTeX tables in Section 9)
suppressWarnings({
  has_knitr      <- requireNamespace("knitr", quietly = TRUE)
  has_kableExtra <- requireNamespace("kableExtra", quietly = TRUE)
})
if (has_knitr)      library(knitr)
if (has_kableExtra) library(kableExtra)

options(tibble.width = Inf)
options(scipen = 999)
Sys.setlocale("LC_TIME", "C")

# Detect project root: if the working directory is "scripts", go one level up.
project_root <- if (basename(getwd()) == "scripts") ".." else "."

data_file <- file.path(project_root, "data", "all_swaps_q1_2026.csv")
out_dir   <- file.path(project_root, "figures")

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)


# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

# Save a plot as both PDF and PNG into the output folder.
save_plot <- function(plot, name, width = 7, height = 4.5, dpi = 300) {
  ggsave(
    filename = file.path(out_dir, paste0(name, ".pdf")),
    plot = plot, width = width, height = height, device = cairo_pdf
  )
  ggsave(
    filename = file.path(out_dir, paste0(name, ".png")),
    plot = plot, width = width, height = height, dpi = dpi
  )
}

# Swiss-style number formatting (apostrophe as thousands separator).
fmt_ch <- function(x, digits = 0) {
  format(round(x, digits), big.mark = "'", scientific = FALSE, trim = TRUE)
}
fmt_usd_ch <- function(x, digits = 0) {
  paste0("$", fmt_ch(x, digits))
}

# Shorten an address / hash for display.
short_addr <- function(x) {
  if_else(is.na(x), NA_character_,
          paste0(substr(x, 1, 6), "...", substr(x, nchar(x) - 3, nchar(x))))
}
short_hash <- function(x) {
  if_else(is.na(x), NA_character_,
          paste0(substr(x, 1, 8), "...", substr(x, nchar(x) - 5, nchar(x))))
}

# Group raw builder labels (from the extraData field) into clean builder names.
group_builder <- function(builder) {
  case_when(
    grepl("Titan",          builder, ignore.case = TRUE) ~ "Titan",
    grepl("Flashbots",      builder, ignore.case = TRUE) ~ "Flashbots",
    grepl("BuilderNet",     builder, ignore.case = TRUE) ~ "BuilderNet",
    grepl("Nethermind",     builder, ignore.case = TRUE) ~ "Nethermind",
    grepl("Quasar",         builder, ignore.case = TRUE) ~ "Quasar",
    grepl("bobTheBuilder",  builder, ignore.case = TRUE) ~ "Bob",
    grepl("beaverbuild",    builder, ignore.case = TRUE) ~ "Beaver",
    grepl("btcs",           builder, ignore.case = TRUE) ~ "BTCS",
    grepl("Eureka",         builder, ignore.case = TRUE) ~ "Eureka",
    is.na(builder) | builder == ""                       ~ "Unknown",
    TRUE                                                  ~ "Other"
  )
}

# Known router / aggregator addresses excluded from the attacker set
# (3 Uniswap routers + 2 1inch routers).
KNOWN_ROUTERS <- c(
  # Uniswap
  "0xe592427a0aece92de3edee1f18e0157c05861564",
  "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",
  "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",
  # 1inch
  "0x111111125421ca6dc452d289314280a0f8842a65",
  "0x1111111254eeb25477b68fb85ed929f73a960582"
)


# ============================================================================
# 1. LOAD DATA
# ============================================================================

data_raw <- read_csv(data_file, show_col_types = FALSE)


# ============================================================================
# 2. FEATURE ENGINEERING
# ============================================================================
# Derive the pool price from sqrtPriceX96, the realized execution price from
# observed token flows, slippage, and a clean builder label.

data <- data_raw %>%
  mutate(
    datetime = as_datetime(timestamp),

    # Uniswap V3 pool price from sqrtPriceX96 (post-trade pool state).
    # USDC has 6 decimals, WETH has 18 decimals.
    price_raw          = (sqrt_price_x96 / 2^96)^2,
    weth_per_usdc      = price_raw * 10^(6 - 18),
    pool_eth_usd_price = 1 / weth_per_usdc,

    # AMM pool price BEFORE the swap (last swap's post-trade price).
    pool_price_before = lag(pool_eth_usd_price),

    # Realized execution price from observed USDC / WETH token flows.
    implied_eth_usd_price = if_else(
      !is.na(weth_amount) & weth_amount != 0,
      abs(usdc_amount / weth_amount),
      NA_real_
    ),

    slippage     = implied_eth_usd_price - pool_eth_usd_price,
    slippage_pct = slippage / pool_eth_usd_price * 100,

    builder = if_else(
      is.na(extra_data_text) | extra_data_text == "",
      "unknown",
      extra_data_text
    )
  )

# Sanity check: required columns must exist.
required_cols <- c(
  "block_number", "tx_hash", "tx_index", "log_index", "sender", "recipient",
  "direction", "amountUSD", "weth_amount_signed", "usdc_amount_signed",
  "priority_fee_gwei", "gas_cost_usd", "pool_eth_usd_price",
  "implied_eth_usd_price", "slippage_pct", "eth_usd_price"
)
missing_cols <- setdiff(required_cols, names(data))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}


# ============================================================================
# 3. DATA OVERVIEW AND QUALITY CHECKS
# ============================================================================

overview <- tibble(
  rows                = nrow(data),
  columns             = ncol(data),
  min_datetime        = min(data$datetime, na.rm = TRUE),
  max_datetime        = max(data$datetime, na.rm = TRUE),
  unique_blocks       = n_distinct(data$block_number),
  unique_transactions = n_distinct(data$tx_hash),
  unique_senders      = n_distinct(data$sender),
  unique_recipients   = n_distinct(data$recipient)
)

direction_overview <- data %>%
  count(direction, sort = TRUE) %>%
  mutate(share_percent = round(n / sum(n) * 100, 2))

print(overview)
print(direction_overview)


# ============================================================================
# 4. COMMON PREPARATION FOR SANDWICH DETECTION
# ============================================================================
# A sandwich needs at least 3 swaps in a block (front, victim, back).
# We also flag transactions that emit exactly one swap event, since
# frontrun transactions are single-swap by construction.

blocks_3plus <- data %>%
  group_by(block_number) %>%
  filter(n() >= 3) %>%
  ungroup()

single_event_txs <- blocks_3plus %>%
  group_by(block_number, tx_hash) %>%
  summarise(n_events = n(), .groups = "drop") %>%
  filter(n_events == 1)

filter_overview <- tibble(
  original_rows       = nrow(data),
  rows_after_filter   = nrow(blocks_3plus),
  original_blocks     = n_distinct(data$block_number),
  blocks_after_filter = n_distinct(blocks_3plus$block_number)
)
print(filter_overview)


# ============================================================================
# 5. GENERAL SANDWICH DETECTION FUNCTION
# ============================================================================
# BUY-side structure:
#   front = BUY_WETH, victim = BUY_WETH, back = SELL_WETH
#   gross profit = back USDC realized - front USDC realized
#
# SELL-side structure:
#   front = SELL_WETH, victim = SELL_WETH, back = BUY_WETH
#   gross profit = front USDC realized - back USDC realized
#
# Backrun transactions sharing one tx_hash are aggregated at the
# transaction-hash level (multiple swap events, single gas payment).

detect_sandwich_side <- function(block_data, single_event_txs,
                                 side = c("buy_side", "sell_side")) {

  side <- match.arg(side)

  if (side == "buy_side") {
    front_direction  <- "BUY_WETH"
    victim_direction <- "BUY_WETH"
    back_direction   <- "SELL_WETH"
  } else {
    front_direction  <- "SELL_WETH"
    victim_direction <- "SELL_WETH"
    back_direction   <- "BUY_WETH"
  }

  # --- Step 1: blocks where a non-router address performs a single-swap
  #             frontrun transaction in the relevant direction.
  valid_attacker_blocks <- block_data %>%
    semi_join(single_event_txs, by = c("block_number", "tx_hash")) %>%
    filter(direction == front_direction) %>%
    distinct(block_number, sender)

  # --- Step 2: candidate attacker-blocks with both a front and a back
  #             direction across at least two distinct transactions.
  attacker_blocks <- block_data %>%
    semi_join(valid_attacker_blocks, by = c("block_number", "sender")) %>%
    filter(!sender %in% KNOWN_ROUTERS) %>%
    group_by(block_number, sender) %>%
    summarise(
      has_front = any(direction == front_direction),
      has_back  = any(direction == back_direction),
      n_tx      = n_distinct(tx_hash),
      .groups   = "drop"
    ) %>%
    filter(has_front, has_back, n_tx >= 2)

  # --- Step 3: frontrun transactions (single-swap).
  fronts <- block_data %>%
    semi_join(attacker_blocks, by = c("block_number", "sender")) %>%
    semi_join(single_event_txs, by = c("block_number", "tx_hash")) %>%
    filter(direction == front_direction) %>%
    rename(attacker = sender) %>%
    group_by(block_number, attacker, tx_hash) %>%
    summarise(
      front_tx_hash            = first(tx_hash),
      front_tx_index           = first(tx_index),
      front_log_index          = first(log_index),
      front_amountUSD          = first(amountUSD),
      front_priority_fee_gwei  = first(priority_fee_gwei),
      front_gas_cost_usd       = first(gas_cost_usd),
      front_pool_price         = first(pool_eth_usd_price),
      front_pool_price_before  = first(pool_price_before),
      front_implied_price      = first(implied_eth_usd_price),
      front_slippage_pct       = first(slippage_pct),
      front_weth_amount_signed = first(weth_amount_signed),
      front_usdc_amount_signed = first(usdc_amount_signed),
      datetime                 = first(datetime),
      .groups = "drop"
    ) %>%
    select(block_number, attacker, datetime, starts_with("front_"))

  # --- Step 4: backrun transactions (aggregated at tx_hash level).
  backs <- block_data %>%
    semi_join(attacker_blocks, by = c("block_number", "sender")) %>%
    filter(direction == back_direction) %>%
    rename(attacker = sender) %>%
    group_by(block_number, attacker, tx_hash) %>%
    summarise(
      back_tx_hash            = first(tx_hash),
      back_tx_index           = first(tx_index),
      back_log_index          = min(log_index),
      back_amountUSD          = sum(amountUSD, na.rm = TRUE),
      back_priority_fee_gwei  = first(priority_fee_gwei),
      back_gas_cost_usd       = first(gas_cost_usd),
      back_pool_price         = pool_eth_usd_price[which.max(log_index)],
      back_pool_price_before  = pool_price_before[which.min(log_index)],
      back_implied_price      = abs(sum(usdc_amount_signed, na.rm = TRUE)) /
                                abs(sum(weth_amount_signed, na.rm = TRUE)),
      back_slippage_pct       = mean(slippage_pct, na.rm = TRUE),
      back_weth_amount_signed = sum(weth_amount_signed, na.rm = TRUE),
      back_usdc_amount_signed = sum(usdc_amount_signed, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    select(block_number, attacker, starts_with("back_"))

  # --- Step 5: pair each frontrun with the closest later backrun.
  pairs <- fronts %>%
    inner_join(backs, by = c("block_number", "attacker"),
               relationship = "many-to-many") %>%
    filter(
      back_tx_index > front_tx_index,
      back_tx_hash != front_tx_hash
    ) %>%
    group_by(block_number, attacker, back_tx_hash) %>%
    slice_max(front_tx_index, with_ties = FALSE) %>%
    ungroup() %>%
    group_by(block_number, attacker, front_tx_hash) %>%
    slice_min(back_tx_index, with_ties = FALSE) %>%
    ungroup() %>%
    mutate(
      local_pair_id = row_number(),
      sandwich_side = side
    )

  # --- Step 6: victim swaps between front and back in the same direction.
  victims_raw <- pairs %>%
    select(local_pair_id, sandwich_side, block_number, attacker,
           front_tx_index, back_tx_index) %>%
    inner_join(
      block_data %>%
        select(
          block_number, tx_hash, tx_index, log_index, sender, recipient,
          direction, amountUSD, pool_eth_usd_price, pool_price_before,
          implied_eth_usd_price, slippage_pct, priority_fee_gwei,
          gas_cost_usd, weth_amount_signed, usdc_amount_signed
        ),
      by = "block_number",
      relationship = "many-to-many"
    ) %>%
    filter(
      tx_index > front_tx_index,
      tx_index < back_tx_index,
      sender != attacker,
      direction == victim_direction
    ) %>%
    rename(
      victim_tx_hash           = tx_hash,
      victim_sender            = sender,
      victim_recipient         = recipient,
      victim_tx_index          = tx_index,
      victim_log_index         = log_index,
      victim_direction         = direction,
      victim_amountUSD         = amountUSD,
      victim_pool_price        = pool_eth_usd_price,
      victim_pool_price_before = pool_price_before,
      victim_implied_price     = implied_eth_usd_price,
      victim_slippage_pct      = slippage_pct,
      victim_priority_fee_gwei = priority_fee_gwei,
      victim_gas_cost_usd      = gas_cost_usd,
      victim_weth_signed       = weth_amount_signed,
      victim_usdc_signed       = usdc_amount_signed
    )

  victim_agg <- victims_raw %>%
    group_by(local_pair_id) %>%
    summarise(
      victim_tx_count             = n_distinct(victim_tx_hash),
      victim_swap_count           = n(),
      victim_volume_usd           = sum(victim_amountUSD, na.rm = TRUE),
      victim_avg_slippage_pct     = mean(victim_slippage_pct, na.rm = TRUE),
      victim_median_slippage_pct  = median(victim_slippage_pct, na.rm = TRUE),
      victim_max_abs_slippage_pct = max(abs(victim_slippage_pct), na.rm = TRUE),
      first_victim_tx_index       = min(victim_tx_index),
      last_victim_tx_index        = max(victim_tx_index),
      .groups = "drop"
    )

  # --- Step 7: profit calculation per sandwich (see Section 3.3 of paper).
  sandwiches <- pairs %>%
    semi_join(victim_agg, by = "local_pair_id") %>%
    left_join(victim_agg, by = "local_pair_id") %>%
    mutate(
      attacker_weth_net = front_weth_amount_signed + back_weth_amount_signed,
      attacker_usdc_net = front_usdc_amount_signed + back_usdc_amount_signed,

      # Close ratio r = |WETH back| / |WETH front|.
      close_ratio = abs(back_weth_amount_signed) / abs(front_weth_amount_signed),

      backrun_type = case_when(
        abs(close_ratio - 1) <= 0.01 ~ "full_close",
        close_ratio < 0.99           ~ "partial_close",
        close_ratio > 1.01           ~ "over_close",
        TRUE                         ~ "unclassified"
      ),

      # Realized fractions (only inventory closed within the sandwich).
      front_usdc_realized = abs(front_usdc_amount_signed) * pmin(close_ratio, 1),
      back_usdc_realized  = abs(back_usdc_amount_signed)  * pmin(1 / close_ratio, 1),
      front_weth_realized = abs(front_weth_amount_signed) * pmin(close_ratio, 1),
      back_weth_realized  = abs(back_weth_amount_signed)  * pmin(1 / close_ratio, 1),

      # SELL-side valuation via realized WETH and execution prices.
      front_usdc_realized_sell = front_weth_realized * front_implied_price,
      back_usdc_realized_sell  = back_weth_realized  * back_implied_price,
      sell_profit_usd          = front_usdc_realized_sell - back_usdc_realized_sell,

      # Gross profit (execution-price based, already net of pool fee + slippage).
      usdc_profit = case_when(
        side == "buy_side"  ~ back_usdc_realized - front_usdc_realized,
        side == "sell_side" ~ sell_profit_usd,
        TRUE ~ NA_real_
      ),
      gross_profit_usd = usdc_profit,

      # Inventory-adjusted control PnL (without scaling).
      inventory_adjusted_profit_usd =
        attacker_usdc_net + attacker_weth_net * back_implied_price,

      attacker_total_gas_cost_usd = front_gas_cost_usd + back_gas_cost_usd,
      net_profit_usd              = gross_profit_usd - attacker_total_gas_cost_usd,
      is_profitable               = net_profit_usd > 0,

      tx_between_count = back_tx_index - front_tx_index - 1,
      tx_distance      = back_tx_index - front_tx_index,
      price_move_front_to_back_pct =
        (back_pool_price - front_pool_price) / front_pool_price * 100,
      attacker_avg_priority_fee_gwei =
        (front_priority_fee_gwei + back_priority_fee_gwei) / 2,

      input_usdc_usd = case_when(
        side == "buy_side"  ~ front_usdc_realized,
        side == "sell_side" ~ back_usdc_realized,
        TRUE ~ NA_real_
      ),
      output_usdc_usd = case_when(
        side == "buy_side"  ~ back_usdc_realized,
        side == "sell_side" ~ front_usdc_realized,
        TRUE ~ NA_real_
      )
    )

  list(
    attacker_blocks = attacker_blocks,
    fronts          = fronts,
    backs           = backs,
    pairs           = pairs,
    victims_raw     = victims_raw,
    victim_agg      = victim_agg,
    sandwiches      = sandwiches
  )
}


# ============================================================================
# 6. RUN BUY-SIDE AND SELL-SIDE DETECTION
# ============================================================================

buy_detection  <- detect_sandwich_side(blocks_3plus, single_event_txs, "buy_side")
sell_detection <- detect_sandwich_side(blocks_3plus, single_event_txs, "sell_side")

buy_sandwiches  <- buy_detection$sandwiches  %>% mutate(sandwich_side = "buy_side")
sell_sandwiches <- sell_detection$sandwiches %>% mutate(sandwich_side = "sell_side")

sandwich_both_sides <- bind_rows(buy_sandwiches, sell_sandwiches) %>%
  mutate(
    pair_id       = row_number(),
    sandwich_side = factor(sandwich_side, levels = c("buy_side", "sell_side")),
    backrun_type  = factor(backrun_type,
                           levels = c("full_close", "partial_close",
                                      "over_close", "unclassified"))
  )

victims_raw_both_sides <- bind_rows(
  buy_detection$victims_raw  %>% mutate(sandwich_side = "buy_side"),
  sell_detection$victims_raw %>% mutate(sandwich_side = "sell_side")
) %>%
  left_join(
    sandwich_both_sides %>% select(pair_id, local_pair_id, sandwich_side),
    by = c("local_pair_id", "sandwich_side")
  )

detection_overview <- tibble(
  total_sandwiches = nrow(sandwich_both_sides),
  buy_side         = nrow(buy_sandwiches),
  sell_side        = nrow(sell_sandwiches),
  unique_attackers = n_distinct(sandwich_both_sides$attacker),
  victim_swaps     = nrow(victims_raw_both_sides),
  unique_victims   = n_distinct(victims_raw_both_sides$victim_sender)
)
print(detection_overview)


# ============================================================================
# 7. TABLE 3 — CLASSIFICATION OF SANDWICH ATTACKS BY SIDE AND BACKRUN TYPE
# ============================================================================

table3_classification <- sandwich_both_sides %>%
  count(sandwich_side, backrun_type, name = "attacks") %>%
  mutate(
    share = round(attacks / sum(attacks) * 100, 1),
    side  = recode(as.character(sandwich_side),
                   buy_side = "Buy-side", sell_side = "Sell-side"),
    type  = recode(as.character(backrun_type),
                   full_close = "Full close", partial_close = "Partial close",
                   over_close = "Over close", unclassified = "Unclassified")
  ) %>%
  arrange(sandwich_side, backrun_type)

cat("\n=== Table 3: Sandwich Attacks by Side and Backrun Type ===\n")
print(as.data.frame(table3_classification %>% select(side, type, attacks, share)))


# ============================================================================
# 8. TABLE 4 — ATTACK SIZE AND PROFITABILITY BY BACKRUN TYPE
# ============================================================================

table4_backrun <- sandwich_both_sides %>%
  filter(backrun_type != "unclassified") %>%
  group_by(backrun_type) %>%
  summarise(
    attacks            = n(),
    median_frontrun    = median(front_amountUSD, na.rm = TRUE),
    median_gas         = median(attacker_total_gas_cost_usd, na.rm = TRUE),
    median_net_profit  = median(net_profit_usd, na.rm = TRUE),
    total_gas          = sum(attacker_total_gas_cost_usd, na.rm = TRUE),
    total_net_profit   = sum(net_profit_usd, na.rm = TRUE),
    pct_profitable     = round(mean(net_profit_usd > 0, na.rm = TRUE) * 100, 1),
    .groups = "drop"
  )

table4_total <- sandwich_both_sides %>%
  filter(backrun_type != "unclassified") %>%
  summarise(
    backrun_type      = "Total",
    attacks           = n(),
    median_frontrun   = median(front_amountUSD, na.rm = TRUE),
    median_gas        = median(attacker_total_gas_cost_usd, na.rm = TRUE),
    median_net_profit = median(net_profit_usd, na.rm = TRUE),
    total_gas         = sum(attacker_total_gas_cost_usd, na.rm = TRUE),
    total_net_profit  = sum(net_profit_usd, na.rm = TRUE),
    pct_profitable    = round(mean(net_profit_usd > 0, na.rm = TRUE) * 100, 1)
  )

table4_complete <- bind_rows(
  table4_backrun %>% mutate(backrun_type = as.character(backrun_type)),
  table4_total
)

cat("\n=== Table 4: Sandwich Attacks by Backrun Type ===\n")
print(as.data.frame(table4_complete))


# ============================================================================
# 9. ROBUSTNESS / PROFIT DECOMPOSITION (AMM PRICES, POOL FEES, SLIPPAGE)
# ============================================================================
# Re-values each leg at the AMM pool price BEFORE the swap, separates pool
# fees (0.05%) and execution slippage, and yields the Table 5 decomposition.

robustness_profit <- sandwich_both_sides %>%
  mutate(
    # AMM-based valuation using pool prices BEFORE each swap.
    front_usdc_realized_amm = front_weth_realized * front_pool_price_before,
    back_usdc_realized_amm  = back_weth_realized  * back_pool_price_before,

    gross_profit_usd_amm = case_when(
      sandwich_side == "buy_side" ~
        back_usdc_realized_amm - front_usdc_realized_amm,
      sandwich_side == "sell_side" ~
        front_usdc_realized_amm - back_usdc_realized_amm,
      TRUE ~ NA_real_
    ),

    # Pool fees (0.05% on frontrun and backrun realized volume).
    pool_fee_rate = 0.0005,
    pool_fees_usd =
      (front_usdc_realized_amm + back_usdc_realized_amm) * pool_fee_rate,

    # Slippage = theoretical AMM profit minus execution gross profit minus fees.
    slippage_loss_usd = gross_profit_usd_amm - gross_profit_usd - pool_fees_usd,

    net_profit_usd_amm =
      gross_profit_usd_amm - attacker_total_gas_cost_usd,
    net_profit_usd_amm_after_fees =
      gross_profit_usd_amm - attacker_total_gas_cost_usd - pool_fees_usd
  )

# --- Table 5: profit decomposition by backrun type.
table5_decomposition <- robustness_profit %>%
  filter(backrun_type != "unclassified") %>%
  group_by(backrun_type) %>%
  summarise(
    amm_profit  = sum(gross_profit_usd_amm, na.rm = TRUE),
    pool_fees   = sum(pool_fees_usd, na.rm = TRUE),
    slippage    = sum(slippage_loss_usd, na.rm = TRUE),
    gross_profit = sum(gross_profit_usd, na.rm = TRUE),
    gas         = sum(attacker_total_gas_cost_usd, na.rm = TRUE),
    net_profit  = sum(net_profit_usd, na.rm = TRUE),
    .groups = "drop"
  )

table5_total <- robustness_profit %>%
  filter(backrun_type != "unclassified") %>%
  summarise(
    backrun_type = "Total",
    amm_profit   = sum(gross_profit_usd_amm, na.rm = TRUE),
    pool_fees    = sum(pool_fees_usd, na.rm = TRUE),
    slippage     = sum(slippage_loss_usd, na.rm = TRUE),
    gross_profit = sum(gross_profit_usd, na.rm = TRUE),
    gas          = sum(attacker_total_gas_cost_usd, na.rm = TRUE),
    net_profit   = sum(net_profit_usd, na.rm = TRUE)
  )

table5_complete <- bind_rows(
  table5_decomposition %>% mutate(backrun_type = as.character(backrun_type)),
  table5_total
)

cat("\n=== Table 5: Profit Decomposition by Backrun Type ===\n")
print(as.data.frame(table5_complete))


# ============================================================================
# 10. ATTACKER SUMMARY, TABLE 6 (TOP 10) AND CONCENTRATION
# ============================================================================

attacker_summary <- sandwich_both_sides %>%
  group_by(attacker) %>%
  summarise(
    sandwich_count          = n(),
    buy_side_count          = sum(sandwich_side == "buy_side"),
    sell_side_count         = sum(sandwich_side == "sell_side"),
    unique_blocks           = n_distinct(block_number),
    total_gross_profit_usd  = sum(gross_profit_usd, na.rm = TRUE),
    total_gas_cost_usd      = sum(attacker_total_gas_cost_usd, na.rm = TRUE),
    total_net_profit_usd    = sum(net_profit_usd, na.rm = TRUE),
    median_net_profit_usd   = median(net_profit_usd, na.rm = TRUE),
    avg_priority_fee_gwei   = mean(attacker_avg_priority_fee_gwei, na.rm = TRUE),
    total_victim_volume_usd = sum(victim_volume_usd, na.rm = TRUE),
    full_close_count        = sum(backrun_type == "full_close"),
    partial_close_count     = sum(backrun_type == "partial_close"),
    over_close_count        = sum(backrun_type == "over_close"),
    .groups = "drop"
  ) %>%
  mutate(
    attacker_short = short_addr(attacker),
    share_percent  = sandwich_count / sum(sandwich_count) * 100
  ) %>%
  arrange(desc(sandwich_count))

table6_top10_attackers <- attacker_summary %>%
  slice_head(n = 10) %>%
  transmute(
    Address      = attacker_short,
    Attacks      = sandwich_count,
    `Gross Profit` = round(total_gross_profit_usd, 0),
    Gas          = round(total_gas_cost_usd, 0),
    `Net Profit` = round(total_net_profit_usd, 0)
  )

cat("\n=== Table 6: Top 10 Sandwich Attackers ===\n")
print(as.data.frame(table6_top10_attackers))

concentration_stats <- tibble(
  unique_attackers      = nrow(attacker_summary),
  top2_share_percent    = sum(attacker_summary$sandwich_count[1:2]) /
                          sum(attacker_summary$sandwich_count) * 100,
  top10_share_percent   = sum(attacker_summary$sandwich_count[1:10]) /
                          sum(attacker_summary$sandwich_count) * 100,
  net_profitable_count  = sum(attacker_summary$total_net_profit_usd > 0),
  gini                  = ineq::Gini(attacker_summary$sandwich_count)
)
cat("\n=== Attacker concentration ===\n")
print(concentration_stats)


# ============================================================================
# 11. VICTIM DAMAGE, SLIPPAGE AND EXECUTION LOSS (TABLES 7 & 8)
# ============================================================================
# Damage uses AMM pool prices: pool price before vs. after the frontrun.
# Slippage uses the victim's own execution price vs. the pre-victim pool price.

victim_damage <- victims_raw_both_sides %>%
  left_join(
    sandwich_both_sides %>%
      select(pair_id, sandwich_side, front_pool_price_before,
             front_pool_price, backrun_type),
    by = c("pair_id", "sandwich_side")
  ) %>%
  mutate(
    counterfactual_pool_price = front_pool_price_before,
    victim_pool_price_used    = front_pool_price,

    # Sandwich damage = |p_v - p_f| * |WETH_v|.
    price_impact_usd =
      abs(victim_pool_price_used - counterfactual_pool_price) *
      abs(victim_weth_signed),
    price_impact_pct =
      abs(victim_pool_price_used - counterfactual_pool_price) /
      counterfactual_pool_price * 100,

    # Victim slippage = |p_e - p_v| * |WETH_v|.
    victim_exec_price = abs(victim_usdc_signed / victim_weth_signed),
    victim_slippage_usd =
      abs(victim_exec_price - victim_pool_price_before) *
      abs(victim_weth_signed),

    is_harmed = price_impact_usd > 0
  )

# --- Table 7: distribution of sandwich damage per victim swap.
table7_damage_distribution <- victim_damage %>%
  summarise(
    Min    = min(price_impact_usd, na.rm = TRUE),
    Q1     = quantile(price_impact_usd, 0.25, na.rm = TRUE),
    Median = median(price_impact_usd, na.rm = TRUE),
    Mean   = mean(price_impact_usd, na.rm = TRUE),
    Q3     = quantile(price_impact_usd, 0.75, na.rm = TRUE),
    Max    = max(price_impact_usd, na.rm = TRUE)
  )
cat("\n=== Table 7: Distribution of Sandwich Damage (USD) ===\n")
print(as.data.frame(table7_damage_distribution))

# --- Table 8: execution loss decomposition.
total_sandwich_damage <- sum(victim_damage$price_impact_usd, na.rm = TRUE)
total_victim_slippage <- sum(victim_damage$victim_slippage_usd, na.rm = TRUE)
total_execution_loss  <- total_sandwich_damage + total_victim_slippage

table8_execution_loss <- tibble(
  Metric = c("Sandwich damage", "Victim slippage", "Total loss"),
  `Damage (USD)` = c(total_sandwich_damage, total_victim_slippage,
                     total_execution_loss),
  `Share of Total` = c(
    total_sandwich_damage / total_execution_loss * 100,
    total_victim_slippage / total_execution_loss * 100,
    100
  )
)
cat("\n=== Table 8: Victim Execution Loss Decomposition ===\n")
print(as.data.frame(table8_execution_loss))

cat("\nSandwich damage as % of victim volume:",
    round(total_sandwich_damage /
            sum(victim_damage$victim_amountUSD, na.rm = TRUE) * 100, 2), "%\n")


# ============================================================================
# 12. TABLE 9 — TOP 10 VICTIMS BY SANDWICH DAMAGE
# ============================================================================

table9_top10_victims <- victim_damage %>%
  group_by(victim_sender) %>%
  summarise(
    attacks          = n(),
    total_volume_usd = sum(victim_amountUSD, na.rm = TRUE),
    total_damage_usd = sum(price_impact_usd, na.rm = TRUE),
    median_damage_usd = median(price_impact_usd, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    damage_pct    = total_damage_usd / total_volume_usd * 100,
    address_short = short_addr(victim_sender)
  ) %>%
  arrange(desc(total_damage_usd)) %>%
  slice_head(n = 10) %>%
  transmute(
    Address        = address_short,
    Attacks        = attacks,
    `Tot. Volume`  = round(total_volume_usd, 0),
    `Tot. Damage`  = round(total_damage_usd, 0),
    `Damage (%)`   = round(damage_pct, 2),
    `Med. Damage`  = round(median_damage_usd, 2)
  )

cat("\n=== Table 9: Top 10 Victims by Sandwich Damage ===\n")
print(as.data.frame(table9_top10_victims))

# --- Robustness: replace router victims by their swap recipient where possible.
victim_damage_recipient_clean <- victim_damage %>%
  mutate(
    victim_address_clean = if_else(
      victim_sender %in% KNOWN_ROUTERS &
        !(victim_recipient %in% KNOWN_ROUTERS),
      victim_recipient,
      victim_sender
    )
  )

# --- Bot-vs-bot overlap: attacker addresses also appearing as victims.
known_attackers <- sandwich_both_sides %>% distinct(attacker) %>% pull(attacker)
overlap_addresses <- intersect(
  unique(victims_raw_both_sides$victim_sender),
  known_attackers
)
cat("\nAttacker addresses also appearing as victims:",
    length(overlap_addresses), "of", length(known_attackers), "\n")
cat("Victim swaps where victim is a known attacker:",
    sum(victims_raw_both_sides$victim_sender %in% known_attackers),
    "of", nrow(victims_raw_both_sides), "\n")


# ============================================================================
# 13. APPENDIX TABLE 10 — VICTIM SWAPS PER ATTACK
# ============================================================================

table10_victims_per_attack <- sandwich_both_sides %>%
  left_join(
    victims_raw_both_sides %>%
      group_by(pair_id) %>%
      summarise(victim_swaps = n(), .groups = "drop"),
    by = "pair_id"
  ) %>%
  mutate(
    victim_swaps = replace_na(victim_swaps, 0),
    bucket = case_when(
      victim_swaps >= 6 ~ "6+",
      TRUE              ~ as.character(victim_swaps)
    ),
    bucket = factor(bucket, levels = c("1", "2", "3", "4", "5", "6+"))
  ) %>%
  count(bucket, name = "attacks") %>%
  mutate(share = round(attacks / sum(attacks) * 100, 1))

cat("\n=== Table 10: Victim Swaps per Sandwich Attack ===\n")
print(as.data.frame(table10_victims_per_attack))


# ============================================================================
# 14. APPENDIX TABLE 11 — FULL-CLOSE SANDWICH ATTACKS
# ============================================================================

table11_full_close <- sandwich_both_sides %>%
  filter(backrun_type == "full_close") %>%
  transmute(
    Block        = block_number,
    Side         = recode(as.character(sandwich_side),
                          buy_side = "BUY-side", sell_side = "SELL-side"),
    Attacker     = short_addr(attacker),
    Frontrun     = round(front_amountUSD, 0),
    Backrun      = round(back_amountUSD, 0),
    `Victim Vol` = round(victim_volume_usd, 0),
    `Close Ratio` = round(close_ratio, 4),
    `Gross Profit` = round(gross_profit_usd, 2),
    Gas          = round(attacker_total_gas_cost_usd, 2),
    `Net Profit` = round(net_profit_usd, 2)
  ) %>%
  arrange(desc(`Net Profit`))

cat("\n=== Table 11: Full-Close Sandwich Attacks ===\n")
print(as.data.frame(table11_full_close))


# ============================================================================
# 15. BUILDER LINKAGE AND TABLE 13 — BUILDER SHARES
# ============================================================================

sandwich_builders <- sandwich_both_sides %>%
  left_join(
    data %>% distinct(block_number, builder, fee_recipient),
    by = "block_number"
  ) %>%
  mutate(
    builder_group       = group_builder(builder),
    builder_short       = substr(builder, 1, 25),
    fee_recipient_short = short_addr(fee_recipient)
  )

# Builder share across all observed swaps.
builder_share_all_swaps <- data %>%
  mutate(builder_group = group_builder(builder)) %>%
  count(builder_group, name = "all_swaps") %>%
  mutate(share_all_swaps = all_swaps / sum(all_swaps) * 100)

# Builder share across detected sandwich attacks.
builder_share_sandwiches <- sandwich_builders %>%
  count(builder_group, name = "sandwiches") %>%
  mutate(share_sandwiches = sandwiches / sum(sandwiches) * 100)

table13_builder_shares <- builder_share_all_swaps %>%
  full_join(builder_share_sandwiches, by = "builder_group") %>%
  mutate(
    all_swaps        = replace_na(all_swaps, 0),
    share_all_swaps  = replace_na(share_all_swaps, 0),
    sandwiches       = replace_na(sandwiches, 0),
    share_sandwiches = replace_na(share_sandwiches, 0)
  ) %>%
  arrange(desc(sandwiches)) %>%
  transmute(
    Builder        = builder_group,
    Swaps          = all_swaps,
    `Share (%)`    = round(share_all_swaps, 1),
    Sandwiches     = sandwiches,
    `Share (%) `   = round(share_sandwiches, 1)
  )

cat("\n=== Table 13: Builder Shares in All Swaps and Sandwich Attacks ===\n")
print(as.data.frame(table13_builder_shares))


# ============================================================================
# 16. APPENDIX TABLE 14 — MEDIAN GAS COSTS ACROSS ATTACKER-BUILDER PAIRS
# ============================================================================

top10_attackers_gas <- sandwich_builders %>%
  count(attacker, name = "total_attacks") %>%
  arrange(desc(total_attacks)) %>%
  slice_head(n = 10)

table14_gas_costs <- sandwich_builders %>%
  filter(attacker %in% top10_attackers_gas$attacker) %>%
  group_by(attacker, builder_group) %>%
  summarise(median_gas_cost_usd = median(attacker_total_gas_cost_usd, na.rm = TRUE),
            .groups = "drop") %>%
  pivot_wider(names_from = builder_group, values_from = median_gas_cost_usd) %>%
  right_join(top10_attackers_gas, by = "attacker") %>%
  mutate(Attacker = short_addr(attacker)) %>%
  arrange(desc(total_attacks)) %>%
  select(-attacker) %>%
  relocate(Attacker, Total = total_attacks)

cat("\n=== Table 14: Median Gas Costs Across Attacker-Builder Pairs ===\n")
print(as.data.frame(table14_gas_costs))


# ============================================================================
# 17. SPEARMAN CORRELATION — DAILY VOLUME VS. SANDWICH ATTACKS
# ============================================================================

daily_volume <- data %>%
  mutate(day = as.Date(datetime)) %>%
  group_by(day) %>%
  summarise(daily_volume_usd = sum(amountUSD, na.rm = TRUE), .groups = "drop")

daily_sandwiches <- sandwich_both_sides %>%
  mutate(day = as.Date(datetime)) %>%
  count(day, name = "sandwich_count")

volume_sandwich_corr <- daily_volume %>%
  left_join(daily_sandwiches, by = "day") %>%
  mutate(sandwich_count = replace_na(sandwich_count, 0))

spearman_result <- cor.test(
  volume_sandwich_corr$daily_volume_usd,
  volume_sandwich_corr$sandwich_count,
  method = "spearman", exact = FALSE
)

cat("\n=== Spearman: daily volume vs. sandwich attacks ===\n")
cat("rho =", round(as.numeric(spearman_result$estimate), 3),
    " p =", signif(spearman_result$p.value, 3),
    " n =", nrow(volume_sandwich_corr), "\n")

daily_volume_summary <- daily_volume %>%
  summarise(
    avg_daily_volume_usd = mean(daily_volume_usd, na.rm = TRUE),
    total_volume_usd     = sum(daily_volume_usd, na.rm = TRUE),
    observations         = n()
  )
cat("Avg daily volume:", fmt_usd_ch(daily_volume_summary$avg_daily_volume_usd, 0),
    " Total volume:", fmt_usd_ch(daily_volume_summary$total_volume_usd, 0), "\n")


# ============================================================================
# 18. FIGURE 1 — DAILY SANDWICH ATTACKS AND ETH PRICE
# ============================================================================

eth_price_daily <- data %>%
  mutate(day = as.Date(datetime)) %>%
  group_by(day) %>%
  summarise(median_eth_price = median(eth_usd_price, na.rm = TRUE), .groups = "drop")

sandwich_time_total <- sandwich_both_sides %>%
  mutate(day = as.Date(datetime)) %>%
  count(day, name = "sandwich_count") %>%
  complete(day = seq(min(day), max(day), by = "day"),
           fill = list(sandwich_count = 0)) %>%
  arrange(day) %>%
  mutate(ma7 = zoo::rollmean(sandwich_count, k = 7, fill = NA, align = "right")) %>%
  left_join(eth_price_daily, by = "day")

avg_sandwiches <- mean(sandwich_time_total$sandwich_count, na.rm = TRUE)

scale_factor <- max(sandwich_time_total$sandwich_count, na.rm = TRUE) /
  max(sandwich_time_total$median_eth_price, na.rm = TRUE)

fig1_sandwiches_over_time <- ggplot(sandwich_time_total, aes(x = day)) +
  geom_col(aes(y = sandwich_count), fill = "grey75", width = 0.8, color = NA) +
  geom_line(aes(y = median_eth_price * scale_factor),
            linewidth = 1, color = "grey35", na.rm = TRUE) +
  geom_hline(yintercept = avg_sandwiches, linetype = "dashed",
             linewidth = 0.6, color = "grey25") +
  annotate("text", x = max(sandwich_time_total$day, na.rm = TRUE),
           y = avg_sandwiches,
           label = paste0("Avg = ", fmt_ch(avg_sandwiches, 1)),
           hjust = 1, vjust = -0.6, size = 3.5, color = "grey25") +
  scale_x_date(date_breaks = "2 weeks", date_labels = "%d %b") +
  scale_y_continuous(
    name   = "Number of Sandwich Attacks",
    labels = function(x) fmt_ch(x, 0),
    expand = expansion(mult = c(0, 0.08)),
    sec.axis = sec_axis(~ . / scale_factor, name = "ETH Price (USD)",
                        labels = function(x) fmt_usd_ch(x, 0))
  ) +
  labs(x = NULL) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"),
        panel.grid.minor = element_blank())

print(fig1_sandwiches_over_time)
save_plot(fig1_sandwiches_over_time, "fig1_sandwiches_over_time")


# ============================================================================
# 19. FIGURE 2 — ATTACKER-BUILDER INTERACTION HEATMAP
# ============================================================================

top_n_attackers <- 10
top_n_builders  <- 10

top_attackers_heatmap <- sandwich_builders %>%
  count(attacker, name = "attacker_total") %>%
  arrange(desc(attacker_total)) %>%
  slice_head(n = top_n_attackers)

top_builders_heatmap <- sandwich_builders %>%
  count(builder_group, name = "builder_total") %>%
  arrange(desc(builder_total)) %>%
  slice_head(n = top_n_builders)

heatmap_data <- sandwich_builders %>%
  filter(attacker %in% top_attackers_heatmap$attacker,
         builder_group %in% top_builders_heatmap$builder_group) %>%
  count(attacker, builder_group, name = "sandwich_count") %>%
  left_join(top_attackers_heatmap, by = "attacker") %>%
  left_join(top_builders_heatmap, by = "builder_group") %>%
  mutate(
    share_within_attacker = sandwich_count / attacker_total * 100,
    attacker_short = short_addr(attacker),
    attacker_label = paste0(attacker_short, " (", attacker_total, ")"),
    builder_label  = paste0(builder_group, " (", builder_total, ")")
  ) %>%
  complete(attacker_label, builder_label,
           fill = list(sandwich_count = 0, share_within_attacker = 0))

attacker_order <- top_attackers_heatmap %>%
  mutate(attacker_label = paste0(short_addr(attacker), " (", attacker_total, ")")) %>%
  pull(attacker_label) %>%
  rev()

builder_order <- top_builders_heatmap %>%
  mutate(builder_label = paste0(builder_group, " (", builder_total, ")")) %>%
  arrange(desc(builder_total)) %>%
  pull(builder_label)

fig2_attacker_builder_heatmap <- heatmap_data %>%
  mutate(
    attacker_label = factor(attacker_label, levels = attacker_order),
    builder_label  = factor(builder_label, levels = builder_order)
  ) %>%
  ggplot(aes(x = builder_label, y = attacker_label, fill = share_within_attacker)) +
  geom_tile(color = "white", linewidth = 0.5) +
  geom_text(aes(label = ifelse(share_within_attacker > 0,
                               paste0(round(share_within_attacker, 0), "%"), "")),
            size = 3, color = "black") +
  scale_fill_gradient(low = "white", high = "grey20",
                      labels = function(x) paste0(round(x), "%")) +
  labs(x = "Builder", y = "Attacker", fill = "Share") +
  theme_minimal(base_size = 12) +
  theme(text = element_text(color = "black"),
        axis.text.x = element_text(angle = 35, hjust = 1),
        panel.grid = element_blank(),
        legend.position = "right")

print(fig2_attacker_builder_heatmap)
save_plot(fig2_attacker_builder_heatmap, "fig2_attacker_builder_heatmap",
          width = 7.8, height = 4.8)


# ============================================================================
# 20. FIGURE 3 — LORENZ CURVE OF ATTACKER CONCENTRATION
# ============================================================================

gini_value <- ineq::Gini(attacker_summary$sandwich_count)
lorenz_obj <- ineq::Lc(attacker_summary$sandwich_count)
lorenz_df  <- tibble(p = lorenz_obj$p, L = lorenz_obj$L)

fig3_lorenz <- ggplot(lorenz_df, aes(x = p, y = L)) +
  geom_line(color = "black", linewidth = 0.6) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed",
              color = "grey50", linewidth = 0.4) +
  annotate("text", x = 0.05, y = 0.95,
           label = paste0("Gini = ", round(gini_value, 2)),
           hjust = 0, vjust = 1, size = 5) +
  labs(x = "Cumulative Share of Attacker Addresses",
       y = "Cumulative Share of Sandwich Attacks") +
  coord_equal(xlim = c(0, 1), ylim = c(0, 1)) +
  theme_minimal(base_size = 12)

print(fig3_lorenz)
save_plot(fig3_lorenz, "fig3_lorenz_attackers", width = 6.5, height = 6.5)

cat("\nAttacker Gini coefficient:", round(gini_value, 3), "\n")


# ============================================================================
# 21. INSPECTION BLOCK — DETAILED VIEW OF A SINGLE BLOCK (SANDWICH EXAMPLES)
# ============================================================================
# Set block_to_check to any block to reproduce the example tables (1, 2, 12).
# The script automatically detects buy-side / sell-side / both / none.

block_to_check <- 24396840  # Example: buy-side full close (paper Table 1)

candidates_to_check <- sandwich_both_sides %>%
  filter(block_number == block_to_check) %>%
  distinct(sandwich_side, block_number, attacker,
           front_tx_hash, back_tx_hash, .keep_all = TRUE) %>%
  arrange(sandwich_side, front_tx_index, back_tx_index)

if (nrow(candidates_to_check) == 0) {
  message("No sandwich candidate found in block ", fmt_ch(block_to_check))
  candidate_to_check        <- tibble()
  sandwich_side_to_check    <- NA_character_
  victim_direction_to_check <- NA_character_
} else {
  message("Detected sandwich side(s) in block ", fmt_ch(block_to_check), ": ",
          paste(unique(as.character(candidates_to_check$sandwich_side)),
                collapse = ", "))
  candidate_to_check     <- candidates_to_check %>% slice(1)
  sandwich_side_to_check <- as.character(candidate_to_check$sandwich_side)
  victim_direction_to_check <- case_when(
    sandwich_side_to_check == "buy_side"  ~ "BUY_WETH",
    sandwich_side_to_check == "sell_side" ~ "SELL_WETH",
    TRUE ~ NA_character_
  )
}

inspect_block <- blocks_3plus %>%
  arrange(block_number, tx_index, log_index) %>%
  group_by(block_number) %>%
  mutate(position_in_block = row_number()) %>%
  ungroup() %>%
  filter(block_number == block_to_check) %>%
  mutate(
    role = if (nrow(candidate_to_check) == 0) {
      rep("Other", n())
    } else {
      case_when(
        tx_hash == candidate_to_check$front_tx_hash ~ "Frontrun Bot",
        tx_hash == candidate_to_check$back_tx_hash  ~ "Backrun Bot",
        tx_index > candidate_to_check$front_tx_index &
          tx_index < candidate_to_check$back_tx_index &
          sender != candidate_to_check$attacker &
          direction == victim_direction_to_check ~ "Victim",
        TRUE ~ "Other"
      )
    },
    address_short   = short_addr(sender),
    tx_hash_short   = short_hash(tx_hash),
    direction_clean = recode(direction, BUY_WETH = "BUY WETH", SELL_WETH = "SELL WETH")
  ) %>%
  select(
    block_number, position_in_block, tx_index, log_index,
    Role = role, Address = address_short, `TX Hash` = tx_hash_short,
    Direction = direction_clean, `Amount USD` = amountUSD,
    `USDC In/Out` = usdc_amount_signed, `WETH In/Out` = weth_amount_signed,
    `Exec. Price` = implied_eth_usd_price, `Pool Price` = pool_eth_usd_price,
    `Priority Fee` = priority_fee_gwei, `Gas USD` = gas_cost_usd
  )

cat("\n=== Inspection: all swaps in block", fmt_ch(block_to_check), "===\n")
print(as.data.frame(inspect_block), digits = 5)


# ============================================================================
# 22. OPTIONAL — EXPORT KEY TABLES AS LATEX
# ============================================================================
# Requires the knitr and kableExtra packages. Set export_latex <- TRUE to run.

export_latex <- FALSE

if (export_latex && has_knitr && has_kableExtra) {

  table6_top10_attackers %>%
    kbl(format = "latex", booktabs = TRUE, escape = FALSE,
        caption = "Top 10 Sandwich Attackers by Number of Detected Attacks",
        label = "tab:top10_attackers") %>%
    kable_styling(latex_options = c("hold_position", "scale_down"),
                  font_size = 9) %>%
    cat(file = file.path(out_dir, "table6_top10_attackers.tex"))

  table13_builder_shares %>%
    kbl(format = "latex", booktabs = TRUE, escape = FALSE,
        caption = "Builder Shares in All Swaps and Detected Sandwich Attacks",
        label = "tab:builder_shares") %>%
    kable_styling(latex_options = c("hold_position", "scale_down"),
                  font_size = 9) %>%
    cat(file = file.path(out_dir, "table13_builder_shares.tex"))

  message("LaTeX tables written to ", out_dir)
}


# ============================================================================
# 23. FINAL OBJECTS FOR MANUAL REVIEW
# ============================================================================

final_objects <- list(
  overview              = overview,
  detection_overview    = detection_overview,
  table3_classification = table3_classification,
  table4_backrun        = table4_complete,
  table5_decomposition  = table5_complete,
  table6_top10          = table6_top10_attackers,
  concentration         = concentration_stats,
  table7_damage         = table7_damage_distribution,
  table8_execution_loss = table8_execution_loss,
  table9_top10_victims  = table9_top10_victims,
  table10_victims       = table10_victims_per_attack,
  table11_full_close    = table11_full_close,
  table13_builders      = table13_builder_shares,
  table14_gas           = table14_gas_costs,
  spearman              = spearman_result,
  attacker_summary      = attacker_summary,
  victim_damage         = victim_damage
)

cat("\n============================================================\n")
cat("Analysis complete. Key reproduced figures:\n")
cat("  Total sandwiches:", nrow(sandwich_both_sides),
    "| buy:", nrow(buy_sandwiches), "| sell:", nrow(sell_sandwiches), "\n")
cat("  Unique attackers:", nrow(attacker_summary),
    "| Top 10 share:", round(concentration_stats$top10_share_percent, 1), "%",
    "| Gini:", round(gini_value, 2), "\n")
cat("  Total net profit: ", fmt_usd_ch(table5_total$net_profit, 0), "\n")
cat("  Total execution loss: ", fmt_usd_ch(total_execution_loss, 0),
    "(sandwich", round(total_sandwich_damage / total_execution_loss * 100, 1),
    "% / slippage", round(total_victim_slippage / total_execution_loss * 100, 1), "%)\n")
cat("============================================================\n")
