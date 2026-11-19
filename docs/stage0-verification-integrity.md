# Stage 0 verification integrity (what PASS does and does not mean)

## Stage statuses
PASS / FAIL / INCONCLUSIVE / NOT_RUN are never interchangeable. Verifier exit code: `0` all required PASS (the only
"verified" code), `1` real failure, `2` full success not proven (Binance HTTP 451 environment block, NOT_RUN, INCONCLUSIVE).
CI: `collect-evidence` uploads evidence and always writes the per-market table; `verification-gate` is green only for exit 0.

## What was hardened
* `evidence_integrity`: events JSONL must exist (missing -> INCONCLUSIVE), be untruncated and valid (FAIL), match the
  collector's envelope count, per-type counts (ws + rest) and the SHA-256 the collector recorded when writing (FAIL).
  `ws_parsed` can never be PASS without it.
* `ws_fresh` is per REQUIRED event type: only LIVE WebSocket events the gate accepted count (no BACKFILL, duplicates,
  out-of-order or bad-timestamp events, no REST events). Window = first socket open .. end of data (slow close excluded).
  Limits are per type (`judge_stream_freshness(limits=...)`, default 20 s). The 20 s default for every type is an
  ASSUMPTION; Toobit snapshot/markPrice cadence is not documented and must be set from observed data.
* `runtime` stage: collector exit 4/5/6 is a FAIL even when the log looks clean.
* `environment_blocked` only when every REST call was HTTP 451 AND every other stage is NOT_RUN.
* `gate_ready=PASS` means only "core types READY at end of data" (scope listed in the report), not source verification.

## Toobit open questions (official docs re-read 2026-11-20, https://api-docs.toobit.com/api/usdt-m-market-data.html)
| Item | Docs say | Observed/inferred | Status |
|---|---|---|---|
| openInterest `size` | "Total open interest of the platform Specific coins, eg.: ETH in ETHUSDT"; example symbol is ETH-SWAP-USDT (wording and symbol disagree); no unit; no link to `contractMultiplier` | none compared against contract spec | stays UNVERIFIED |
| index vs BTC-SWAP-USDT | endpoint keyed by BTCUSDT; exchangeInfo contracts carry `"index": "BTCUSDT"`, which suggests the mapping but does not state it | price similarity is not evidence | stays UNVERIFIED (check exchangeInfo `index` field equals the queried name) |
| diffDepth | page covers REST only; WS sequence semantics not documented here (adapter notes: versions not guaranteed unique) | - | UNVERIFIED; no full order-book reconstruction is claimed |
