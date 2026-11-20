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

## Toobit index price / open interest: evidence in three separate layers
Sources are kept apart: (1) official documentation, (2) the official support answer, (3) our own live observation.

### 1. Official documentation (https://api-docs.toobit.com/api/usdt-m-market-data.html, re-read 2026-11-20)
* exchangeInfo example shows contract `"index": "BTCUSDT"`; the index map is keyed `BTCUSDT`; no field table.
* openInterest `size`: "Total open interest of the platform Specific coins, eg.: ETH in ETHUSDT"; no explicit unit.
* diffDepth: WS sequence semantics not documented (versions not guaranteed unique) -> UNVERIFIED, out of scope.

### 2. Official support answer
```
Provider: Toobit Support
Ticket: #20261120000097
Reply date: 2026-11-20 15:56:04
```
**Confirmed by support**
* `exchangeInfo.indexToken` is the recommended index lookup field; for BTC, `indexToken` corresponds to `BTCUSDT`;
  it is used as the `symbol` of `GET /quote/v1/index` (e.g. `?symbol=BTCUSDT`).
* `openInterestList[].size` represents base-token quantity; the contract coefficient is ALREADY applied to `size`
  (`base_token_quantity = contract_quantity x contract_coefficient`). Never multiply or divide it again.

**Still unconfirmed**
* The exact live JSON path of `indexToken` (support did not give one) - independently observed below, see layer 3.
* Whether the older `index` field needs a documentation correction.
* Whether OI aggregates one side or both long and short positions, and the double-count rules (`oi_aggregation`).
* Whether the WebSocket `index` topic uses the same `indexToken` (the answer names the REST endpoint only; `ws_index_symbol`).

### 3. Live observation (our own public, unauthenticated GETs; observation is not an official answer)
2026-11-20 (`/api/v1/exchangeInfo`, `/quote/v1/index`, `/quote/v1/openInterest`) and re-checked 2026-11-21:
* JSON path: `exchangeInfo.contracts[].indexToken` (string). Present on 795 of 795 contracts, none empty, no duplicate symbols.
* `BTC-SWAP-USDT`: `index` = `"BTC"` (the bare coin), `indexToken` = `"BTCUSDT"`, `quoteAsset` = `"USDT"`, `contractMultiplier` = `"0.001"`.
* `GET /quote/v1/index?symbol=BTCUSDT` -> `index`/`edp` keyed exactly `BTCUSDT`; `?symbol=BTC` -> empty maps. This matches the
  support rule; it is evidence the rule holds, not a replacement for the per-run check below.
* 13 contracts have `indexToken != index + quoteAsset` (e.g. `1000PEPE-SWAP-USDT` -> `PEPEUSDT`, `LUNA2-SWAP-USDT` ->
  `LUNAUSDT`): a `base+quote` name rule is wrong and is never used. Whether the index PRICE of such a 1000x contract is on the
  contract's price scale is NOT confirmed; Stage 0 verifies BTC only.
* OI: response symbol equals the requested contract; sizes are multiples of neither `contractMultiplier` nor LOT `stepSize`.
  That granularity neither confirms nor excludes a unit (the unit comes from the support answer only).

### What the code does with it
`toobit.SEMANTIC_EVIDENCE` records the three answered questions (`index_contract_mapping = INDEX_TOKEN_FIELD`,
`oi_size_unit = BASE_ASSET`, `oi_contract_multiplier = ALREADY_APPLIED`) with ticket, date and quote. `oi_aggregation` and
`ws_index_symbol` stay `None` (open). Unit/coefficient and aggregation are separate states and never one combined verdict.

`check_rest` cross-checks both rows against the exchangeInfo response of the SAME run (`live.SEMANTIC_CHECKS`).
exchangeInfo is requested BEFORE the index row; `/quote/v1/index` is requested with that contract's own `indexToken`:
* index: no usable `indexToken` (missing, empty, not a plain string, or contract absent / exchangeInfo unavailable) -> the
  index is NOT requested (no fallback guess) and the row is INCONCLUSIVE; duplicate contract entries -> FAIL; response keys
  other than the requested token, or an invalid (<=0 / NaN / Inf / non-numeric) index or edp -> FAIL; empty map, missing
  edp entry or official rule not on record -> INCONCLUSIVE. PASS only if the response is keyed by exactly that contract's
  `indexToken` and the rule is on record. The legacy `index` field and price similarity are never evidence.
* openInterest: item for another symbol, duplicate items, size < 0 / NaN / Inf / non-numeric -> FAIL (value sanity).
  Sanity OK with unit and coefficient confirmed but aggregation open -> INCONCLUSIVE (reason lists the open question).
  PASS only when every OI answer is on record AND `VERIFICATION["rest_open_interest"]` is deliberately set to DOC.
  `size` is kept as sent (string in `raw_payload`, exact `Decimal` in the normalized payload); no `contractMultiplier`
  conversion in either direction.

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
| `extended_semantics_status` | Toobit INDEX_PRICE contract mapping (indexToken), OPEN_INTEREST unit/coefficient/aggregation | the two semantic REST rows PASS, `indexPriceComponents` evidence valid, and every official question answered (still open: `oi_aggregation`, `ws_index_symbol`); NOT_APPLICABLE for Binance |

* A type is excluded from core ONLY if core never uses it (not in `live.CORE_TYPES`, not a required WS type). Each exclusion
  is listed in `excluded_from_core` with its status and reason; if core used it, it could not be excluded.
* `downstream_restrictions` is printed while extended semantics are not PASS. A core PASS is not full verification and
  not trading approval.

## Isolation of unverified Toobit data
Audit: nothing outside `src/stage0` imports stage-0 data (no strategy / signal / order path). Inside stage 0 the values
reach the quality gate, the JSONL evidence and the reports only.
* INDEX_PRICE envelopes: `contract_mapping: VERIFIED` (no mapping flag, filed under the CONTRACT's common symbol, payload
  `contract_symbol`) ONLY when the caller proves the contract's `indexToken` equals the response key (REST check and
  `poll_reference`, both resolve it from the same run's exchangeInfo) and the official rule is on record. Everything else
  - envelopes without that proof, and every WebSocket index push - carries `UNVERIFIED_CONTRACT_MAPPING`,
  `contract_mapping: UNVERIFIED`, `index_name` = the exchange key; raw payload kept.
  The WS `index` topic subscription needs the `indexToken` as its symbol (a contract symbol is rejected, never converted).
* OPEN_INTEREST envelopes: `unit: BASE_ASSET`, `contract_coefficient_already_applied: VERIFIED` (support ticket) but
  `aggregation: UNVERIFIED` -> flags `UNVERIFIED_SCHEMA` + `UNVERIFIED_AGGREGATION` (and `UNVERIFIED_UNIT` only if the unit /
  coefficient record is missing or malformed); `contract_multiplier_applied: false`, `comparable_across_exchanges: false`;
  raw `size` kept, never rescaled. The gate treats them as UNVERIFIED (never READY, no cross-exchange comparison or sum).
* Gate: any flag in `envelope.UNVERIFIED_FLAGS` -> type UNVERIFIED (never READY, `usable()` False); `divergence()` also
  refuses an event carrying such a flag even if readiness were READY (defence in depth).
* Note on observations: value granularity (sizes not multiples of `contractMultiplier` / `stepSize`) and price
  similarity neither confirm nor exclude a unit or mapping.

## Recording an official Toobit answer
`toobit.SEMANTIC_EVIDENCE` holds one record per question in `toobit.SEMANTIC_QUESTIONS`: `index_contract_mapping`,
`ws_index_symbol`, `oi_size_unit`, `oi_contract_multiplier`, `oi_aggregation`. Answered (support ticket #20261120000097):
the first, third and fourth. Open: `ws_index_symbol`, `oi_aggregation`. A record needs `value` (from `SEMANTIC_ALLOWED`),
`source`, `date`, `reference` (ticket / doc URL) and a verbatim `quote`; a malformed record fails closed (FAIL,
INVALID_EVIDENCE). The effective level is `toobit.level(key)`: DOC only if `VERIFICATION[key]` is DOC AND every required
answer is recorded, so editing `VERIFICATION` alone never promotes (`rest_open_interest` and `ws_index` stay UNVERIFIED).
Add a regression test with each record.

`GET /quote/v1/indexPriceComponents?symbol=<indexToken>` (docs: symbol = "Index name, like BTCUSDT"; the token comes from the
same run's exchangeInfo, not requested if unresolved) is run by `--check` as
extended evidence only (`extended_evidence`, not in the REST contract, never changes the --check exit code). Live
2026-11-20: valid `index` / `edp` / `time`, 6 weighted components; `?symbol=BTC` -> `{}`. It validates the BTCUSDT
index, never the contract mapping (`proves_contract_mapping: false`).
