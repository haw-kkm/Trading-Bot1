# Implementation Plan: Phase 1 Core Architecture & Safety Engine (Stage 0, 5, 6, 7)

## Overview

This implementation plan establishes the foundational architecture and safety mechanisms for the Multi-Agent Cryptocurrency Trading System, focusing on Phase 1 components:
- **Stage 0**: Multi-source data ingestion and schema standardization (Binance Spot, Binance Futures, Toobit) with unified `AgentEnvelope`.
- **Stage 5**: Hard-coded `RiskEngine` with deterministic validation pipeline (Reject / Clamp) against tamper-evident limits.
- **Stage 6**: `OrderGuardian` with race condition-resistant order lifecycle state machine and exchange-side order monitoring.
- **Stage 7**: Execution State Machine and **Hybrid Emergency Rollback Protocol** ensuring fail-safe position management and SL/TP automation on Toobit.

---

## 1. Detailed Architectural Specifications

### 1.1. OrderGuardian Race Condition State Machine (`Stage 6`)
To prevent race conditions where an order fill occurs simultaneously with a cancellation request, the order lifecycle state machine defines explicit transition rules:

```
[CREATED] ---> [SUBMITTED] ---> [OPEN]
                                   |
                       +-----------+-----------+
                       | (Cancel Requested)    | (Exchange Match/Fill)
                       v                       v
               [PENDING_CANCEL] -------------> [FILLED / PARTIALLY_FILLED]
                       |                        (Fill takes absolute precedence)
                       | (Cancel Confirmed)     |
                       v                        v
                 [CANCELLED]             [PROCESS_POSITION & SL/TP]
```

- **States**: `CREATED`, `SUBMITTED`, `OPEN`, `PENDING_CANCEL`, `FILLED`, `PARTIALLY_FILLED`, `CANCELLED`, `REJECTED`, `EMERGENCY_CLOSING`.
- **Race Resolution Rule**: If an order is in `PENDING_CANCEL` and an execution fill (`FILLED` or `PARTIALLY_FILLED`) arrives via WebSocket before cancel confirmation, **Fill takes absolute precedence**. The state immediately transitions to `FILLED` (or updates partial fill), records `RACE_CONDITION_FILL_RESOLVED`, and instantly invokes position processing and SL/TP registration.

### 1.2. RiskEngine Deterministic Validation Pipeline (`Stage 5`)
All trade intents from LLMs or strategy agents pass through a strict, hard-coded validation pipeline before reaching `OrderGuardian`:
1. **Schema & Boundary Check**: Validate JSON structure, types, and positive numerical ranges.
2. **Global Hard Limit Verification**: Compare against tamper-evident local config (`config/risk-limits.json` with startup SHA-256 checksum verification).
3. **Decision Matrix**:
   - **REJECT**: Triggered if proposed leverage $L > L_{max}$, direction violates global limits, or account is under drawdown circuit breaker. Emits `RISK_REJECTED`.
   - **CLAMP**: Triggered if proposed position size $S$ exceeds max allowable position for account equity or available margin (while leverage is within limits). Mathematically caps $S = \min(S_{proposed}, S_{allowable})$, logs `RISK_CLAMPED`, and proceeds.

### 1.3. Hybrid Emergency Rollback Protocol (`Stage 7`)
When an order fills on Toobit but immediate server-side SL/TP registration fails or protection status cannot be verified:
1. **New Entry Freeze**: Immediately block new order submissions and request cancellation of pending entry orders.
2. **Conditional Retry & IOC Limit Exit**:
   - Under stable market conditions and within `rollback_timeout_ms` (1000ms), attempt up to `max_sl_tp_retries` (2 retries, 200ms delay) to register SL/TP.
   - If SL/TP registration fails after retries, issue a **Reduce-Only IOC Limit Order** at best bid/ask (with `max_slippage_pct: 0.005`) to exit gracefully.
3. **Market Close Fallback**: If the IOC limit order remains unfilled or partially filled, immediately execute a **Reduce-Only Market Order** (`MARKET_CLOSE`) on the remaining position.
4. **Extreme Conditions Bypass**: In extreme volatility, communication faults, or risk threshold breaches, skip retries/IOC and execute immediate emergency market liquidation.
5. **Idempotency & Position Reconciliation**: Query Toobit `/v1/position` API to reconcile actual open positions before and after closing, preventing duplicate or inverse orders.
6. **Panic StateLock**: Enter `EMERGENCY_LOCK` state, halting all automated trading and LLM inputs until position size is verified at 0 and manual operator reset is performed.

---

## 2. Task Breakdown & Execution Steps

### Task 1: Multi-Source Ingestion & Unified Schema (`Stage 0`)
- [ ] **1.1**: Implement `AgentEnvelope` schema with timestamp (`received_at`), reliability score, source identifier (`binance-spot`, `binance-futures`, `toobit`), and hypothesis ID.
- [ ] **1.2**: Implement WebSocket clients for Binance Spot & Futures order books / trades.
- [ ] **1.3**: Implement Toobit WebSocket & REST client for execution feeds.
- [ ] **1.4**: Implement price divergence detector between Binance analysis feeds and Toobit execution feeds with configurable threshold alerting.

### Task 2: Tamper-Evident RiskEngine (`Stage 5`)
- [ ] **2.1**: Create `config/risk-limits.json` containing immutable limits (`max_leverage`, `max_position_size_pct`, `max_drawdown_pct`, `max_sl_tp_retries`, `rollback_timeout_ms`, `max_slippage_pct`).
- [ ] **2.2**: Implement startup checksum verification (SHA-256) for `risk-limits.json`.
- [ ] **2.3**: Build deterministic validation pipeline supporting strict `REJECT` and `CLAMP` logic.
- [ ] **2.4**: Write unit tests verifying that LLM-proposed excessive leverage/sizes are correctly rejected or clamped.

### Task 3: OrderGuardian State Machine & Race Condition Defense (`Stage 6`)
- [ ] **3.1**: Implement order lifecycle state machine (`CREATED` through `CANCELLED`/`FILLED`/`EMERGENCY_CLOSING`).
- [ ] **3.2**: Implement race condition handler where incoming `FILLED` events override `PENDING_CANCEL` states.
- [ ] **3.3**: Implement active order monitoring and volatility circuit breakers.
- [ ] **3.4**: Write unit tests simulating simultaneous cancel requests and WebSocket fill reports.

### Task 4: Execution Engine & Hybrid Emergency Rollback (`Stage 7`)
- [ ] **4.1**: Implement Toobit order execution client supporting `reduce-only`, `IOC`, and market orders.
- [ ] **4.2**: Implement automated SL/TP registration upon fill confirmation.
- [ ] **4.3**: Implement Hybrid Emergency Rollback workflow (Failure detection $\rightarrow$ short retry $\rightarrow$ IOC limit exit $\rightarrow$ market close fallback).
- [ ] **4.4**: Implement `EMERGENCY_LOCK` state machine, exchange position reconciliation, and manual operator reset mechanism.
- [ ] **4.5**: Write integration tests simulating SL/TP API failure and verifying safe emergency liquidation and state locking.

---

## 3. Verification & Acceptance Criteria

1. **Schema Compliance**: All ingested data across Binance and Toobit successfully wraps in `AgentEnvelope` with 100% schema validation.
2. **RiskEngine Determinism**: 100% of test cases with excessive leverage are rejected, and excessive position sizes are clamped deterministically.
3. **Race Condition Resilience**: Simulated simultaneous cancel and fill events correctly resolve to fill state without duplicate order submission or unhandled exceptions.
4. **Emergency Rollback Safety**: Simulated SL/TP registration failures successfully trigger entry freeze, retry/IOC fallback, position reconciliation, and panic state locking.

## Stage 0 status (see docs/plans/stage0-data.md)
Stage 0 data layer implemented in Python (src/stage0) with fixture-based tests; DATA-01..05 are implemented for FIXTURE mode only. No live data was collected; Toobit shapes unverified. Not wired to order execution.
