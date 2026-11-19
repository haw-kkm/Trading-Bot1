# Roadmap: Multi-agent Cryptocurrency Trading System

## Phases

- [ ] **Phase 1: Multi-Source Data Ingestion & Schema Normalization** - Ingest and normalize Binance Spot, Binance Futures, and Toobit feeds with unified AgentEnvelope schema and divergence detection.
- [ ] **Phase 2: Core Execution Engine & Deterministic Risk Guardrails** - Build PaperExchange, non-bypassable RiskEngine, OrderGuardian, and execution state machine with resilience testing.
- [ ] **Phase 3: Parallel Data Agents & Live Integration** - Implement 8 parallel Data Agents with mock data and transition them to live API feeds with regression testing.
- [ ] **Phase 4: Adversarial Analysis, LLM Decision Making & End-to-End Paper Trading** - Implement adversarial analysts, risk proposal agents, LLM decider, and execute end-to-end paper trading verification.

## Phase Details

### Phase 1: Multi-Source Data Ingestion & Schema Normalization
**Goal**: Robust, normalized real-time data streams and schema standards across Binance Spot, Futures, and Toobit.
**Depends on**: Nothing (first phase)
**Requirements**: DATA-01, DATA-02, DATA-03, DATA-04, DATA-05
**Success Criteria** (what must be TRUE):
  1. System successfully connects to Binance Spot, Binance Futures, and Toobit WebSockets/REST APIs and normalizes order books, prices, and trades into unified structures.
  2. `AgentEnvelope` correctly encapsulates raw data, metadata, `received_at` timestamps, reliability scores, and hypothesis IDs across all streams.
  3. Multi-source validation detects network connection drops, abnormal data latency, and price divergence between Binance and Toobit in real time.
**Plans**: TBD

### Phase 2: Core Execution Engine & Deterministic Risk Guardrails
**Goal**: Immutable risk boundaries, order guardians, and paper execution simulation protecting against abnormal market conditions and race conditions.
**Depends on**: Phase 1
**Requirements**: CORE-01, CORE-02, CORE-03, CORE-04, CORE-05
**Success Criteria** (what must be TRUE):
  1. `PaperExchange` accurately simulates order lifecycles (partial fills, cancellations, stop-loss/take-profit triggers) without hitting live exchange APIs.
  2. `RiskEngine` enforces hard-coded limits on drawdown, leverage, and position sizing that cannot be bypassed or modified by agents or LLMs.
  3. `OrderGuardian` continuously monitors open orders and order-book volatility, automatically cancelling orders when invalidation criteria or race conditions occur.
  4. Execution State Machine cleanly manages order submissions, state transitions, and server-side SL/TP registration with robust WebSocket reconnection resilience.
**Plans**: TBD

### Phase 3: Parallel Data Agents & Live Integration
**Goal**: 8 specialized parallel data agents providing multi-dimensional market intelligence from mock data to live API feeds.
**Depends on**: Phase 1, Phase 2
**Requirements**: AGNT-01, AGNT-02
**Success Criteria** (what must be TRUE):
  1. All 8 Data Agents (Trend, Whale Position, Retail Congestion, Liquidation Map, Open Interest, Candle/Indicator, Fear & Greed, Overfitting Control) run in parallel using mock data inputs.
  2. Live API integration for all 8 Data Agents successfully passes independent regression testing against live exchange feeds.
**Plans**: TBD

### Phase 4: Adversarial Analysis, LLM Decision Making & End-to-End Paper Trading
**Goal**: Complete adversarial multi-agent reasoning, risk proposal, LLM decision making, and end-to-end paper trading verification.
**Depends on**: Phase 3
**Requirements**: LLM-01, LLM-02, LLM-03, LLM-04
**Success Criteria** (what must be TRUE):
  1. Long vs Short adversarial analysts (Stage 2) successfully execute independent analysis and cross-rebuttal rounds.
  2. Risk Proposal agents (Stage 3) generate aggressive, neutral, and conservative parameter configurations.
  3. Deep Thinking LLM decision maker (Stage 4) produces valid entry/sideline decisions and If-Then limit order hypotheses with clear invalidation conditions.
  4. End-to-end paper trading pipeline executes full multi-agent decision loops while successfully enforcing hard-coded safety gates.
**Plans**: TBD

## Progress Table

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Multi-Source Data Ingestion & Schema Normalization | 0/0 | Not started | - |
| 2. Core Execution Engine & Deterministic Risk Guardrails | 0/0 | Not started | - |
| 3. Parallel Data Agents & Live Integration | 0/0 | Not started | - |
| 4. Adversarial Analysis, LLM Decision Making & End-to-End Paper Trading | 0/0 | Not started | - |
