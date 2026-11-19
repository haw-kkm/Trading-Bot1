# Stage 0 — market-data collection (implementation status)

Naming (do not confuse): **Stage 0** = this data layer (`src/stage0/`, DATA-01..05). **Phase 1** = the
roadmap phase that contains it. **P0-1 / P0-2 / P1-1 / P1-2** = earlier order-safety defect fixes
(`src/core`, `src/orders`), untouched by Stage 0.

Stack note: older design text recommends Node.js/TypeScript; the real implementation is **Python 3**
(+ PostgreSQL 16 for order safety). Python is the stack of record.

## What exists
| Area | File | State |
|---|---|---|
| DATA-04 envelope | `src/stage0/envelope.py` | implemented + tested (Decimal, UTC-only, digest tamper check, schema version) |
| DATA-01 Binance Spot | `src/stage0/adapters/binance.py` | parsers implemented, fixture-tested; shapes from Binance public docs **not live-verified** |
| DATA-02 Binance Futures | same file | parsers implemented (trade, depth, mark/index, funding, OI, long/short ratio, forceOrder, exchangeInfo); liquidation *heatmap/estimates*: `ESTIMATED_DATA_UNAVAILABLE` (not implemented) |
| DATA-03 Toobit Futures | `src/stage0/adapters/toobit.py` | parsers on an **ASSUMED, UNVERIFIED** shape; every event is flagged `UNVERIFIED_SCHEMA`; fixtures are synthetic |
| order book | `src/stage0/orderbook.py` | Binance snapshot+diff procedure (spot `U==u+1`, futures `pu==u`), gap/crossed -> INVALID, resync |
| DATA-05 gate | `src/stage0/quality.py` | READY/STALE/DISCONNECTED/SYNCING/INVALID/UNVERIFIED; duplicate/out-of-order/future/lag/crossed; divergence with freshness + basis guard |
| runner | `src/stage0/collector.py` | FIXTURE replay (default, no network); PUBLIC_READ_ONLY plumbing (policy, allow-listed GET paths, reconnect backoff, subscription plan); **streaming loop not implemented** |
| auth interface | `src/stage0/account_interface.py` | interface only; raises `NotConfigured`, never fakes data |

## Not done / unverified
* No real exchange traffic was ever generated. **Real-time collection is NOT verified.**
* Toobit: official docs were not consulted (network ban). Symbol format `BTC-SWAP-USDT` is from the task text.
  Before enabling anything: check each parser against official docs/real samples, record URL + date here,
  then (and only then) drop the `UNVERIFIED_SCHEMA` flag and allow-list paths.
* Binance field layouts were written from knowledge of the public docs, not re-checked against live responses
  (doc URLs: developers.binance.com spot / usds-margined-futures; date checked: none — offline).
* No WebSocket transport, no live REST pollers, no persistence of envelopes.
* Stage 0 is **not wired** into ExecutionEngine/RiskEngine; its `usable()` gate is an interface for future use.

## FIXTURE run
`python -m src.stage0.collector tests/fixtures/stage0/session_basic.jsonl`

## Needed later for real connectivity
User approval for network (Binance first; Toobit separately), WebSocket transport, official-doc verification
of Toobit shapes, clock-offset sync via `/time`, persistence, monitoring.

## Deferred safety findings (NOT changed in Stage 0)
1. `src/orders/submitter.py` ACK validation: a response with only `orderId` may be treated as accepted even if the
   exchange reports a rejection status. Risk: rejected order believed live. Needed: strict status check + regression tests.
2. `src/core/risk_engine.py`: hard-coded risk inputs (`open_buy_positions = 2`, `default_price = 50000.0`) are not real
   data. Risk: wrong limits/clamps. Needed: real positions/price source + regression tests.
Plus the earlier unresolved ones: PROTECTION_UNKNOWN, authority-to-wire race, SINGLE_ACCOUNT_ONLY, no exchange exactly-once.
