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
Live public GETs compared on 2026-11-20 (`/api/v1/exchangeInfo`, `/quote/v1/index`, `/quote/v1/openInterest`; no keys).

| Item | Docs say | Observed live (2026-11-20) | Status |
|---|---|---|---|
| index vs BTC-SWAP-USDT | exchangeInfo example: contract `"index": "BTCUSDT"`; index map keyed `BTCUSDT`; no field table | contract `index` is the bare coin (`"BTC"`, all 792 contracts); `?symbol=BTCUSDT` -> key `BTCUSDT`; `?symbol=BTC` and `?symbol=BTC-SWAP-USDT` -> empty maps. Docs example and live disagree | stays UNVERIFIED -> REST row INCONCLUSIVE |
| openInterest `size` | "Total open interest of the platform Specific coins, eg.: ETH in ETHUSDT"; example symbol ETH-SWAP-USDT; no explicit unit; nothing about `contractMultiplier` | response symbol equals the requested contract; sizes are multiples of neither `contractMultiplier` nor LOT `stepSize` (checked for multipliers 0.0001..100) -> neither "contract count" nor "sum of base-coin lots" confirmed | stays UNVERIFIED -> REST row INCONCLUSIVE; never rescaled |
| diffDepth | page covers REST only; WS sequence semantics not documented here (adapter notes: versions not guaranteed unique) | - | UNVERIFIED; no full order-book reconstruction is claimed (out of scope) |

`check_rest` cross-checks both rows against the exchangeInfo response of the SAME run (`live.SEMANTIC_CHECKS`):
* index: response keys other than the requested index name, or an invalid (<=0 / NaN / Inf / non-numeric) index or edp
  -> FAIL; empty map, missing contract / `index` field, contract `index` != response key, failed exchangeInfo -> INCONCLUSIVE;
  PASS only if the contract's own `index` field names exactly the response key.
* openInterest: item for another symbol, duplicate items, size < 0 / NaN / Inf / non-numeric -> FAIL (value sanity);
  sanity OK -> INCONCLUSIVE while the UNIT is UNVERIFIED. Unit metadata (`BASE_ASSET`) is attached only after a
  deliberate `VERIFICATION["rest_open_interest"] = DOC` edit backed by new evidence.

## Verification chain (second hardening pass)
`collector process -> collector summary -> JSONL evidence -> verifier -> Actions gate`; a missing or contradictory link blocks PASS.
* REST: `judge_rest(check, rows, required)` needs the check subprocess to finish (no timeout, exit 0, or 1 only when a row FAILs),
  every required endpoint exactly once (contract = `live.required_rest_endpoints`, derived from the same table `check_rest`
  executes), unknown endpoints never substitute. `environment_blocked` additionally requires a complete 451-only report.
* Evidence PASS needs: file, valid envelopes, summary count, per-type counts, collector-attested line count AND SHA-256, both
  equal to the file. Missing attestation -> INCONCLUSIVE; any mismatch/corruption -> FAIL. The file is scanned as a stream
  (one line at a time, 16 MiB per-line cap, incremental SHA-256).
* Collector subprocess exit code must equal the summary `exit_code`; otherwise every WS stage is FAIL.
