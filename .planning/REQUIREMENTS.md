# Requirements: Multi-agent Cryptocurrency Trading System

**Defined:** 2026-11-19
**Core Value:** Safely translate predictive signals into precise, risk-controlled orders on Toobit using hard-coded safety guardrails (RiskEngine & OrderGuardian) and verified multi-source data (Binance Spot/Futures + Toobit).

## v1 Requirements

### Stage 0 & Data Engineering (DATA)
- [ ] **DATA-01**: Ingest and normalize Binance Spot market data (price, order book, actual trades, order book imbalance) separately from Futures.
- [ ] **DATA-02**: Ingest and normalize Binance USDT-M Futures data (price, OI, funding rate, long/short ratio, liquidation map with direct vs estimated source separation).
- [ ] **DATA-03**: Ingest and normalize Toobit Futures data (live price, order book, spread, Mark Price, Index Price, contract specs, position state).
- [ ] **DATA-04**: Implement `AgentEnvelope` common schema including raw data vs agent interpretation, `event_time`, `received_at`, `source`, `reliability_score`, `schema_version`, and `hypothesis_id`.
- [ ] **DATA-05**: Implement multi-source validation detecting data latency, connection drops, and abnormal price divergences between Binance and Toobit.

### Phase 1 Core Engine & Safety (CORE)
- [ ] **CORE-01**: Implement `PaperExchange` simulation environment that blocks real exchange API calls and simulates order lifecycle (partial fills, cancellations, SL/TP execution).
- [ ] **CORE-02**: Implement deterministic `RiskEngine` (Stage 5) enforcing hard limits on maximum drawdown, leverage, and position sizing that the LLM cannot bypass.
- [ ] **CORE-03**: Implement `OrderGuardian` (Stage 6) to monitor open limit orders, market volatility, and race conditions (cancel vs fill), automatically cancelling orders when invalidation conditions are met.
- [ ] **CORE-04**: Implement `Execution State Machine` (Stage 7) managing order submission, race condition defense, and server-side SL/TP registration where supported.
- [ ] **CORE-05**: Implement resilience testing for WebSocket disconnection, duplicate events, and order state ambiguity.

### Phase 2 Data Agents & Pipeline (AGNT)
- [ ] **AGNT-01**: Implement 8 parallel Data Agents (Trend, Whale Position, Retail Congestion, Liquidation Map, Open Interest, Candle/Indicator, Fear & Greed, Overfitting Control Simulation) using Mock data.
- [ ] **AGNT-02**: Sequentially integrate live APIs for the 8 Data Agents with independent regression testing.

### Phase 3 Adversarial & LLM Integration (LLM)
- [ ] **LLM-01**: Implement Stage 2 Long vs Short adversarial analysts (1R independent analysis -> 2R cross-rebuttal).
- [ ] **LLM-02**: Implement Stage 3 Risk Proposal agents (aggressive, neutral, conservative parameter generation).
- [ ] **LLM-03**: Implement Stage 4 Deep Thinking LLM decision maker (entry/sideline decisions, If-Then limit order hypotheses and invalidation conditions).
- [ ] **LLM-04**: Execute end-to-end paper trading verification pipeline with safety gate enforcement.

## v2 Requirements

### Advanced Scaling
- **ADV-01**: High-frequency streaming optimization with NATS/Kafka distributed message bus.
- **ADV-02**: Dynamic volatility-adjusted risk parameter tuning.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Microsecond HFT execution | System is built for positional and algorithmic trading, not microsecond HFT |
| Direct LLM order execution | All orders must pass through hard-coded RiskEngine and OrderGuardian |
| Automated API key management | Manual API credential setup is required for security |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DATA-01 | Phase 1 | Pending |
| DATA-02 | Phase 1 | Pending |
| DATA-03 | Phase 1 | Pending |
| DATA-04 | Phase 1 | Pending |
| DATA-05 | Phase 1 | Pending |
| CORE-01 | Phase 2 | Pending |
| CORE-02 | Phase 2 | Pending |
| CORE-03 | Phase 2 | Pending |
| CORE-04 | Phase 2 | Pending |
| CORE-05 | Phase 2 | Pending |
| AGNT-01 | Phase 3 | Pending |
| AGNT-02 | Phase 3 | Pending |
| LLM-01 | Phase 4 | Pending |
| LLM-02 | Phase 4 | Pending |
| LLM-03 | Phase 4 | Pending |
| LLM-04 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 16 total
- Mapped to phases: 16
- Unmapped: 0 ✓

---
*Requirements defined: 2026-11-19*
*Last updated: 2026-11-19 after detailed prompt integration*
## Status note
DATA-01..05: code + fixture tests exist (src/stage0); live verification NOT done; Toobit (DATA-03) parsers unverified. Boxes above intentionally left unchecked until live verification.
