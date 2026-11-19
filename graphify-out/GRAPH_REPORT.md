# Graph Report - Trading bot  (2026-11-19)

## Corpus Check
- Corpus is ~27,376 words - fits in a single context window. You may not need a graph.

## Summary
- 739 nodes · 1957 edges · 26 communities (18 shown, 8 thin omitted)
- Extraction: 84% EXTRACTED · 16% INFERRED · 0% AMBIGUOUS · INFERRED: 304 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 4
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12
- Community 13
- Community 14
- Community 15
- Community 16
- Community 17
- Community 18
- Community 19
- Community 20
- Community 21
- Community 22
- Community 23
- Community 24

## God Nodes (most connected - your core abstractions)
1. `ExecutionEngine` - 88 edges
2. `PaperExchange` - 50 edges
3. `OrderStore` - 48 edges
4. `RiskEngine` - 46 edges
5. `SafeSubmitter` - 45 edges
6. `IntentState` - 40 edges
7. `OrderGuardian` - 36 edges
8. `TradeHypothesis` - 32 edges
9. `EmergencyLockState` - 28 edges
10. `OrderStore` - 27 edges

## Surprising Connections (you probably didn't know these)
- `Recommended stack: Node.js/TypeScript, NestJS, Redis, PostgreSQL, InfluxDB, CCXT` --semantically_similar_to--> `Python dependencies: psycopg, pytest, pixeltable-pgserver`  [AMBIGUOUS] [semantically similar]
  .planning/research/STACK.md → requirements.txt
- `test_execute_without_token_raises_and_never_reaches_client()` --uses--> `UnauthorizedExecutionError`  [INFERRED]
  tests/unit/test_stage8_safety.py → src/core/execution_engine.py
- `test_emergency_lock_state()` --uses--> `ExecutionState`  [INFERRED]
  tests/unit/test_stage7_execution.py → src/core/execution_engine.py
- `test_execution_engine_initial_state()` --uses--> `ExecutionState`  [INFERRED]
  tests/unit/test_stage7_execution.py → src/core/execution_engine.py
- `test_integration_sl_tp_failure_emergency_liquidation()` --uses--> `ExecutionState`  [INFERRED]
  tests/unit/test_stage7_execution.py → src/core/execution_engine.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Roadmap phase dependency chain** — planning_roadmap_phase1, planning_roadmap_phase2, planning_roadmap_phase3, planning_roadmap_phase4 [INFERRED 0.85]
- **Non-bypassable safety pipeline** — planning_requirements_riskengine, planning_requirements_orderguardian, planning_requirements_execution_state_machine [INFERRED 0.85]

## Communities (26 total, 8 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.06
Nodes (34): AuthorizationAlreadyConsumed, IllegalTransition, IntentState, OrderIntent, OrderStore, ExchangeRejected, SafeSubmitter, _valid_ack() (+26 more)

### Community 1 - "Community 1"
Cohesion: 0.06
Nodes (51): ExchangeRejected, PersistenceFailed, IntentState, SafeSubmitter, SubmissionResult, store(), test_concurrent_processes_single_winner(), _engine() (+43 more)

### Community 2 - "Community 2"
Cohesion: 0.06
Nodes (10): ActiveEmergencyOrderExists, AuthorizationAlreadyConsumed, EntriesBlocked, IllegalTransition, LiveTradingDisabled, StaleClaim, _intent(), OrderIntent (+2 more)

### Community 3 - "Community 3"
Cohesion: 0.07
Nodes (13): SourceId, TradeHypothesis, RiskEngine, client_order_id_for(), test_risk_engine_accept_valid_hypothesis(), test_risk_engine_calculate_risk_metrics(), test_risk_engine_checksum_verification(), test_risk_engine_clamp_excessive_position() (+5 more)

### Community 4 - "Community 4"
Cohesion: 0.08
Nodes (34): EmergencyLockState, PaperExchange, _engine(), _entry_order(), _kinds(), _locked(), test_ack_success_but_fill_unconfirmed_sends_nothing_more(), test_adapter_without_account_api_blocks_rollback_orders() (+26 more)

### Community 5 - "Community 5"
Cohesion: 0.09
Nodes (22): _issue_entry(), _ok_sender(), OpenTx, _pending_exit(), Runner, _spawn(), test_claim_for_submit_vs_concurrent_unknown(), test_entry_blocked_when_unknown_transition_is_uncommitted_and_lands_first() (+14 more)

### Community 6 - "Community 6"
Cohesion: 0.05
Nodes (41): AgentEnvelope schema (Stage 0), EMERGENCY_LOCK panic state, Hybrid Emergency Rollback Protocol, OrderGuardian race condition state machine, Phase 1 Core Architecture & Safety Engine plan, Price divergence detector, RiskEngine validation pipeline (REJECT/CLAMP, SHA-256 risk-limits.json), No exactly-once placement at exchange (+33 more)

### Community 7 - "Community 7"
Cohesion: 0.09
Nodes (5): AgentEnvelope, test_agent_envelope_creation_success(), test_agent_envelope_invalid_field_values(), test_agent_envelope_missing_required_fields(), test_trade_hypothesis_creation()

### Community 8 - "Community 8"
Cohesion: 0.16
Nodes (12): OrderGuardian, test_cancel_confirmed_transitions_to_cancelled(), test_cancel_order_transitions_to_pending_cancel(), test_emergency_closing_state(), test_fill_takes_precedence_over_cancel(), test_handle_fill_without_engine_warns_and_does_not_silently_die(), test_order_state_transitions(), test_race_condition_fill_overrides_pending_cancel() (+4 more)

### Community 9 - "Community 9"
Cohesion: 0.11
Nodes (9): classify_fill(), ExecutionState, FillAssessment, FillState, PolicyViolation, check_policy(), ExecutionContext, OrderPurpose (+1 more)

### Community 10 - "Community 10"
Cohesion: 0.11
Nodes (14): AlertSink, test_order_guardian_wires_execution_engine_and_alert_sink(), _clamped_token(), _filled_guardian(), _hypothesis(), test_client_none_does_not_die_silently(), test_engine_uses_clamped_quantity_not_caller_quantity(), test_execute_client_exception_is_submission_unknown_not_rollback() (+6 more)

### Community 11 - "Community 11"
Cohesion: 0.13
Nodes (5): ExecutionEngine, test_hybrid_emergency_rollback_entry_freeze(), test_hybrid_emergency_rollback_retry_sl_tp(), test_emergency_rollback_uses_actual_position_not_caller_values(), test_rollback_itself_never_raises_when_client_missing()

### Community 12 - "Community 12"
Cohesion: 0.12
Nodes (8): UnauthorizedExecutionError, ValidatedOrder, test_emergency_lock_state(), test_toobit_order_execution_ioc(), test_toobit_order_execution_reduce_only(), test_execute_with_forged_token_raises(), test_sha256_forgery_with_known_algorithm_is_rejected(), test_unknown_and_entry_freeze_survive_restart()

### Community 13 - "Community 13"
Cohesion: 0.17
Nodes (11): _make_validated_order(), _paper(), test_execution_engine_initial_state(), test_extreme_conditions_bypass(), test_hybrid_emergency_rollback_ioc_exit(), test_hybrid_emergency_rollback_market_close_fallback(), test_integration_sl_tp_failure_emergency_liquidation(), test_manual_operator_reset() (+3 more)

### Community 14 - "Community 14"
Cohesion: 0.16
Nodes (5): _make_validated_order(), MockToobitClient, test_handle_fill_emergency_closing_on_sl_tp_failure(), test_order_guardian_initial_state(), test_position_processing_on_fill()

### Community 15 - "Community 15"
Cohesion: 0.23
Nodes (7): migrate(), _default_engine_store(), factory(), pg_dsn(), _reset_tables(), _server_dsn(), store()

### Community 16 - "Community 16"
Cohesion: 0.19
Nodes (3): Order, OrderEvent, OrderState

### Community 18 - "Community 18"
Cohesion: 0.15
Nodes (3): MockToobitClient, test_manual_operator_reset_refuses_when_adapter_lacks_account_queries(), test_position_reconciliation()

## Ambiguous Edges - Review These
- `Recommended stack: Node.js/TypeScript, NestJS, Redis, PostgreSQL, InfluxDB, CCXT` → `Python dependencies: psycopg, pytest, pixeltable-pgserver`  [AMBIGUOUS]
  .planning/research/STACK.md · relation: semantically_similar_to

## Knowledge Gaps
- **20 isolated node(s):** `Multi-Exchange decision (Binance analysis, Toobit execution)`, `Hard-coded Safety decision`, `8 Data Agents, 2 Adversarial Analysts, 3 Risk Proposal Agents, LLM Decider`, `Out of Scope: HFT, credential automation, direct LLM trading`, `PaperExchange` (+15 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 257 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **8 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ExecutionEngine` connect `Community 11` to `Community 1`, `Community 2`, `Community 3`, `Community 4`, `Community 5`, `Community 8`, `Community 9`, `Community 10`, `Community 12`, `Community 13`, `Community 14`, `Community 15`, `Community 16`, `Community 17`, `Community 18`, `Community 19`?**
  _High betweenness centrality (0.209) - this node is a cross-community bridge._
- **Are the 14 inferred relationships involving `ExecutionEngine` (e.g. with `AlertSink` and `ValidatedOrder`) actually correct?**
  _`ExecutionEngine` has 14 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Multi-Exchange decision (Binance analysis, Toobit execution)`, `Hard-coded Safety decision`, `8 Data Agents, 2 Adversarial Analysts, 3 Risk Proposal Agents, LLM Decider` to the rest of the system?**
  _20 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.05637840420449116 - nodes in this community are weakly interconnected._
- **Why does `RiskEngine` connect `Community 3` to `Community 1`, `Community 4`, `Community 5`, `Community 8`, `Community 9`, `Community 10`, `Community 11`, `Community 12`, `Community 13`, `Community 14`, `Community 16`?**
  _High betweenness centrality (0.091) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `OrderStore` (e.g. with `ActiveEmergencyOrderExists` and `AuthorizationAlreadyConsumed`) actually correct?**
  _`OrderStore` has 6 INFERRED edges - model-reasoned connections that need verification._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `Recommended stack: Node.js/TypeScript, NestJS, Redis, PostgreSQL, InfluxDB, CCXT` and `Python dependencies: psycopg, pytest, pixeltable-pgserver`?**
  _Edge tagged AMBIGUOUS (relation: semantically_similar_to) - confidence is low._