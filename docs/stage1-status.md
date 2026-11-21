# Stage 1 status (analysis agents) — offline / fixture-based

**Stage 0 FULL: INCONCLUSIVE · Stage 0 CORE: PASS · LIVE TRADING: NOT APPROVED**

Basis for starting Stage 1 (user-run live verification on Windows): binance-spot PASS, binance-futures PASS,
toobit-futures INCONCLUSIVE; core_collection=PASS, extended_semantics=INCONCLUSIVE, overall=INCONCLUSIVE (exit 2).
Open Toobit items stay open: OI aggregation basis, WS index mapping, diffDepth sequence semantics.
Stage 1 records this status (`Stage0Context`) but never uses it to promote an agent: each agent is READY only if
its **own** event types are READY in the Stage 0 `DataQualityGate`.

## Architecture (`src/stage1/`)
| File | Role |
|---|---|
| `models.py` | `AgentResult` (one schema for all 8), `AgentStatus`, `AgentInput` (unforgeable), `Stage0Context` |
| `data_access.py` | `GatedDataAccess`: replays `MarketEnvelope`s through a fresh Stage 0 gate up to `now` (no future data), withholds non-READY types |
| `quality_policy.py` | source/symbol validation, exchange-symbol round trip, UNVERIFIED/BACKFILL/FUTURE quarantine, per-source semantic registry |
| `agents/*.py` | the 8 agents; `indicators.py` pure Decimal math; `series.py` KLINE-or-trade-derived bars |
| `runner.py` | `python -m src.stage1.runner <events.jsonl> --source S --symbol SYM [--stage0-recorded]` |

Confidence = data-sufficiency (completeness × min input reliability; DEGRADED capped at 0.7; 0 when unusable).
It is not a forecast accuracy or return. Nothing in `src/stage1` imports order, account, risk or network code (tested).

## Agent status
| # | Agent | Implemented | Data used | Verified on real data | Missing | READY? |
|---|---|---|---|---|---|---|
| ① | Trend | yes | KLINE (closed) or TRADE-derived bars | fixtures only; real evidence has no KLINE and < 20 one-minute trade bars | exchange candles, ≥ 20 bars | not on current evidence (DATA_UNAVAILABLE); trade-derived bars cap at DEGRADED |
| ② | Whale Position | partial | observed large TRADE prints (Binance only) | evidence run: DEGRADED | account positions, public top-trader stats; Toobit trade unit unconfirmed (UNVERIFIED) | never (positions unobservable) |
| ③ | Retail Position Change | gated | LONG_SHORT_RATIO | evidence: UNVERIFIED | population / update period not in the envelope | no — UNVERIFIED until metadata confirmed |
| ④ | Liquidation Map | observed-only | LIQUIDATION (forceOrder) | not collected in Stage 0 evidence | liquidation stream collection, exchange-wide positions (heat-map withheld) | no (DATA_UNAVAILABLE / DEGRADED) |
| ⑤ | Open Interest | yes | Binance Futures OPEN_INTEREST | evidence: DEGRADED (single observation) | ≥ 2 OI observations for change | Binance: yes with ≥ 2 obs; Toobit: UNVERIFIED (aggregation) |
| ⑥ | Funding & Technical | yes | FUNDING_RATE + KLINE / trades | fixtures; evidence DEGRADED (too few bars) | KLINE feed or longer trade window (34 bars for MACD) | yes on fixtures with KLINE |
| ⑦ | Fear & Greed | stub | none | n/a | verified public source | no — always DATA_UNAVAILABLE |
| ⑧ | Dynamic Simulation | yes (offline) | verified prices | fixtures; evidence DEGRADED | KLINE / ≥ 15 bars for volatility scenario | yes on fixtures; output is simulated, never a fill |

**Stage 1 overall: NOT READY.** Having 8 agent files does not complete Stage 1.

## Next
Collect KLINE (and a longer trade window) in Stage 0 live runs; collect Binance `forceOrder`; document the Binance
long/short-ratio population/period and carry it in the envelope; resolve Toobit OI aggregation; pick a verified
fear & greed source (needs explicit approval); then re-run on live evidence.
