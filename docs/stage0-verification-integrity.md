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

## Verification scope: core_collection vs extended_semantics (2026-11-20)
The report keeps `overall` / exit code exactly as before (full verification; Toobit stays INCONCLUSIVE, exit 2 while the
questions below are open) and adds two scoped results next to it:

| Field | Covers | PASS requires |
|---|---|---|
| `core_collection_status` | Core Public Market Data Collection | REST rows the core uses (contract complete, integrity preconditions on the FULL check report), required WS types received, parsing, per-type freshness, collector exit 0, JSONL line count + SHA-256 attestation, gate READY, and every type the core uses verified (`end_of_collection.per_type` OK, none UNVERIFIED) |
| `extended_semantics_status` | Toobit INDEX_PRICE contract mapping, OPEN_INTEREST unit/multiplier/aggregation | the two semantic REST rows PASS, `indexPriceComponents` evidence valid, and every official question answered; NOT_APPLICABLE for Binance |

* A type is excluded from core ONLY if core never uses it (not in `live.CORE_TYPES`, not a required WS type). Each exclusion
  is listed in `excluded_from_core` with its status and reason; if core used it, it could not be excluded.
* `downstream_restrictions` is printed while extended semantics are not PASS. A core PASS is not full verification and
  not trading approval.

## Isolation of unverified Toobit data
Audit: nothing outside `src/stage0` imports stage-0 data (no strategy / signal / order path). Inside stage 0 the values
reach the quality gate, the JSONL evidence and the reports only.
* INDEX_PRICE envelopes (REST and WS): `UNVERIFIED_SCHEMA` + `UNVERIFIED_CONTRACT_MAPPING`, payload `contract_mapping:
  UNVERIFIED`, `index_name` = the exchange key; raw payload kept. Filed under the contract's common symbol for bookkeeping only.
* OPEN_INTEREST envelopes: `UNVERIFIED_SCHEMA` + `UNVERIFIED_UNIT`, payload `unit: UNVERIFIED`, `aggregation: UNVERIFIED`,
  `contract_multiplier_applied: false`, `comparable_across_exchanges: false`; raw `size` kept, never rescaled.
* Gate: any flag in `envelope.UNVERIFIED_FLAGS` -> type UNVERIFIED (never READY, `usable()` False); `divergence()` also
  refuses an event carrying such a flag even if readiness were READY (defence in depth).
* Note on observations: value granularity (sizes not multiples of `contractMultiplier` / `stepSize`) and price
  similarity neither confirm nor exclude a unit or mapping.

## Recording an official Toobit answer (not done; registry is empty)
`toobit.SEMANTIC_EVIDENCE` holds one record per question in `toobit.SEMANTIC_QUESTIONS`:
`index_contract_mapping`, `oi_size_unit`, `oi_contract_multiplier`, `oi_aggregation`. A record needs `value` (from
`SEMANTIC_ALLOWED`), `source`, `date`, `reference` (ticket / doc URL) and a verbatim `quote`; a malformed record fails
closed (FAIL, INVALID_EVIDENCE). The effective level is `toobit.level(key)`: DOC only if `VERIFICATION[key]` is DOC AND
every required answer is recorded, so editing `VERIFICATION` alone never promotes. Add a regression test with each record.

`GET /quote/v1/indexPriceComponents?symbol=BTCUSDT` (docs: symbol = "Index name, like BTCUSDT") is run by `--check` as
extended evidence only (`extended_evidence`, not in the REST contract, never changes the --check exit code). Live
2026-11-20: valid `index` / `edp` / `time`, 6 weighted components; `?symbol=BTC` -> `{}`. It validates the BTCUSDT
index, never the contract mapping (`proves_contract_mapping: false`).
