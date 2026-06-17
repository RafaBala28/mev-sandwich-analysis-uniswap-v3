# MEV Sandwich Analysis on Uniswap V3

This repository contains the code and supporting material for the seminar paper:

**"An Economic Analysis of MEV Extraction on Uniswap V3: Sandwich Attacks and Bot Profitability"**

conducted at the University of Basel under the supervision of Prof. Dr. Fabian Schär.

---

## Overview

Maximal Extractable Value (MEV) has become an important feature of decentralized finance (DeFi). This project investigates sandwich attacks in the Uniswap V3 WETH/USDC 0.05% pool on Ethereum.

The analysis focuses on:

- Sandwich attack detection
- Attacker profitability
- Victim losses
- Builder-searcher relationships
- Market concentration among attackers and builders

---

## Research Questions

1. How prevalent are sandwich attacks in the Uniswap V3 WETH/USDC pool?

2. How profitable are sandwich attacks and what factors drive profitability?

3. What economic losses do sandwich attacks impose on victim traders?

4. To what extent are sandwich attacks concentrated among a small number of attackers and builders?

---

## Dataset

- Blockchain: Ethereum Mainnet
- Protocol: Uniswap V3
- Pool: WETH/USDC
- Fee Tier: 0.05%
- Observation Period: Q1 2026

Data were collected directly from Ethereum using public RPC providers and processed from raw Uniswap V3 swap events.

---

## Methodology

The analysis follows four main steps:

### 1. Data Collection

- Extraction of swap events from Ethereum
- Decoding of Uniswap V3 Swap events
- Calculation of execution prices and gas costs

### 2. Sandwich Detection

Sandwich attacks are identified by detecting:

- Frontrun transaction
- Victim transaction(s)
- Backrun transaction

within the same Ethereum block.

### 3. Profitability Analysis

Attacks are classified as:

- Full Close
- Partial Close
- Over Close

Profits are calculated using realized inventory only and adjusted for:

- Slippage
- Pool Fees
- Gas Costs

### 4. Victim Loss Analysis

Victim losses are decomposed into:

- Sandwich Damage
- Victim Slippage
- Total Execution Loss

---

## Main Findings

- Sandwich attacks are a persistent feature of Uniswap V3 trading activity.
- Attack frequency increases during periods of high volatility.
- Most attacks are not fully closed within the same block.
- Execution slippage is the largest factor reducing attacker profitability.
- Victims suffer meaningful execution losses.
- Sandwich activity is highly concentrated among a small number of attackers and builders.

---

## Repository Structure

```text
paper/
    Seminar_Paper.pdf

data_collection/
    collect_swaps.py
    decode_swap_events.py

analysis/
    01_data_cleaning.R
    02_sandwich_detection.R
    03_profit_calculation.R
    04_victim_damage.R
    05_builder_analysis.R

figures/
```

## Technologies

- Python
- R
- Ethereum RPC
- Uniswap V3
- dplyr
- ggplot2
- Blockchain Analytics

---

## Author

Rafael Balasteguim da Silva

University of Basel

MSc Business and Economics
