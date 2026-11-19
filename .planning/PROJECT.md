# Multi-agent Cryptocurrency Trading System

## What This Is
A multi-agent automated cryptocurrency trading system that integrates multi-source market data (Binance Spot/Futures) with a dedicated execution exchange (Toobit). The system employs an adversarial analysis framework, risk management engines, and LLM-based decision making to automate high-frequency or positional trading strategies.

## Core Value
The system must safely translate predictive signals derived from comprehensive market data (Spot/Futures) into precise, risk-controlled orders on the execution exchange (Toobit) using hard-coded safety guardrails that cannot be overridden by the LLM.

## Requirements

### Validated
(None yet — ship to validate)

### Active
- [ ] Stage 0: Multi-source data ingestion and schema standardization (Binance Spot, Binance Futures, Toobit)
- [ ] Stage 5: Hard-coded RiskEngine (Loss limits, leverage, position sizing)
- [ ] Stage 6: OrderGuardian (Real-time Toobit order validation and market emergency cancellation)
- [ ] Stage 7: Execution State Machine (Toobit order management and SL/TP automation)

### Out of Scope
- High-frequency HFT execution — system is designed for positional/algorithmic trading, not microsecond-level HFT.
- Automated API credential management — manual setup required for security.
- Direct LLM trading — all trades must pass through hard-coded RiskEngine and OrderGuardian.

## Context
- **Market Dynamics**: Discrepancies between Binance (analysis) and Toobit (execution) prices require normalization and strict contract-spec validation.
- **Data Integrity**: Independent data streams for Spot, Futures, and Execution exchange to ensure source reliability and latency tracking.
- **Safety**: Hard-coded safety layers (Stage 5/6) operate independently of LLM decision-making to prevent "runaway" trading.

## Constraints
- **Data Source**: Binance (Analysis) vs. Toobit (Execution).
- **Architecture**: 8 parallel Data Agents, 2 Adversarial Analysts, 3 Risk Proposal Agents, LLM Decider.
- **Security**: Hard-coded safety engine must be non-bypassable by LLM.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Multi-Exchange | Analysis on liquid Binance, execution on Toobit | — Pending |
| Hard-coded Safety | Prevent AI decision errors via immutable logic | — Pending |

---
*Last updated: 2026-11-19 after initialization*

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

## Stage 0 status (see docs/plans/stage0-data.md)
Stage 0 data layer implemented in Python (src/stage0) with fixture-based tests; DATA-01..05 are implemented for FIXTURE mode only. No live data was collected; Toobit shapes unverified. Not wired to order execution.
