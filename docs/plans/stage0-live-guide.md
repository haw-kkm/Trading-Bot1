# Stage 0 public-data connection guide (user-run)

Everything here is PUBLIC, UNAUTHENTICATED market data. No API key is needed, read, or attached to a request,
even if keys exist in your environment or `.env` (the Stage 0 code never reads the environment or `.env`).
Authenticated account/order APIs are out of scope and are not called.

**Status labels — keep them apart**
| Claim | State |
|---|---|
| Offline tests pass | yes — CI run 44556886774 @ 46e67b1 (423 tests, 5/5 mutants killed); later runs listed in the final report |
| Code can talk to Binance/Toobit | **NOT verified** — nobody has run it against a real exchange yet |
| Parsers match real responses | **NOT verified** — only the official docs were compared |

## Setup
```
pip install -r requirements.txt     # includes `websockets` (live runner only)
```

## Commands (run in this order; each is separate evidence)
1. Binance REST shape check (GET only):
   `python -m src.stage0.live --check --source binance-futures --symbol BTCUSDT --allow-binance-network`
2. Binance 30 s collection:
   `python -m src.stage0.live --source binance-futures --symbol BTCUSDT --seconds 30 --allow-binance-network --out binance.jsonl`
   (Spot: `--source binance-spot`.)
3. Toobit — only if YOU approve public read-only access (both flags are mandatory, otherwise it refuses):
   `python -m src.stage0.live --check --source toobit-futures --symbol BTC-SWAP-USDT --allow-toobit-network --i-approve-toobit-public-readonly`
   `python -m src.stage0.live --source toobit-futures --symbol BTC-SWAP-USDT --seconds 30 --allow-toobit-network --i-approve-toobit-public-readonly --out toobit.jsonl`
   The code constant `TOOBIT_NETWORK_APPROVED` stays False; your approval is only the run-time flag.

## How to read the result
**`--check`** prints one row per endpoint and exits 0 (no FAIL) or 1 (at least one FAIL):
* `PASS` — response received, required structure present, parser produced events, data non-empty.
* `FAIL` — request failed (HTTP status / timeout / not JSON) or the structure contradicts the parser
  (`missing_keys` lists what was absent; `extra_keys` lists undocumented additions, informational only).
* `INCONCLUSIVE` — empty response, no events for the symbol, odd server time, or the docs leave the meaning/unit open
  (Toobit index and open interest never become PASS automatically). `structure: PASS` + INCONCLUSIVE = shape matches, meaning unverified.
The report shows keys and counts, never response values or credentials.

**Collection** exits with:
| Code | Meaning | What to do |
|---|---|---|
| 0 | ran for the full time, healthy | inspect the `--out` file |
| 2 | refused: missing network flag / Toobit approval | add the flags shown in the message |
| 3 | required REST data (server time, snapshot) unavailable | check connectivity/region blocks; re-run `--check` |
| 4 | reconnect attempts exhausted | network problem or endpoint change; see `issues` |
| 5 | data-integrity / internal error (repeated book gaps, permanent HTTP error) | send the JSON summary for analysis |
| 6 | finished but UNHEALTHY: no events, >50 % unparsable, or a stall longer than `--stall-seconds` (20) | not a success; see `unhealthy_reasons` |
The JSON summary prints `issues`, `rest_failures` (endpoint/kind/status/attempts only), `final_per_type` (OK / STALE / MISSING / UNVERIFIED per data type).
Check the output file: `wc -l binance.jsonl` and `head -c 600 binance.jsonl` (each line is one MarketEnvelope JSON).

## Endpoint facts used (re-verified 2026-11-20 against official docs)
* Binance Spot WS: `wss://stream.binance.com:9443/stream?streams=...` (docs: `stream?streams=` combined format).
* Binance USDS-M Futures WS is split by path (legacy `/ws`, `/stream` announced for removal 2026-04-23):
  `/public/stream` = bookTicker + depth; `/market/stream` = aggTrade, markPrice, kline, forceOrder.
  The runner therefore opens TWO futures connections. `trade` is not listed under any futures path, so aggTrade is used.
  Source: developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
* Toobit REST `https://api.toobit.com` (docs basic-information); WS `wss://stream.toobit.com/quote/ws/v1`;
  ping `{"ping": <ms>}` / pong `{"pong": <ms>}`; rate-limit header `X-Api-Limit-Reset-Timestamp` (ms epoch) honoured on 429.
* REST errors: 429/418/5xx/timeouts retried at most 4 times with exponential back-off + jitter, honouring `Retry-After` /
  `X-Api-Limit-Reset-Timestamp`; a requested wait > 60 s aborts instead. Other 4xx (incl. 403 WAF) are never retried.

## What each Toobit data type may claim
| Type | Level | Why |
|---|---|---|
| REST price, trades, depth, bookTicker, markPrice, fundingRate, exchangeInfo contracts; WS trade, markPrice, depth, bookTicker, kline | DOC | structure matches docs; no live comparison yet |
| REST/WS index | UNVERIFIED | docs don't relate the `BTCUSDT`-style key to `BTC-SWAP-USDT` |
| REST openInterest | UNVERIFIED | `size` has no documented unit |
| WS diffDepth | UNVERIFIED | version numbers "not guaranteed unique": no gap detection, never builds a book |
UNVERIFIED data makes `DataQualityGate.status()` return UNVERIFIED (never READY) for consumers that need it.

## Gate semantics (DATA-05)
Freshness is per (source, symbol, data type). `usable(source, symbol, now, types=[...])` is READY only if every
requested type is present, fresh (streaming 10 s; slower reference types longer), has a sane timestamp (future / large
receive lag does not count as fresh) and is not UNVERIFIED. After a disconnect, streaming data received before it no longer
counts: READY returns only after all required types arrive again (and the book is re-synced).
A failed optional REST reference (e.g. open interest) never stops the stream, but consumers needing that type stay non-READY.

## Known unverified points
* Whether the code actually connects (DNS/TLS/geo-blocks/rate limits) — untested.
* Real response shapes vs docs; Toobit WS trade `m` ("true = buy" per docs; Binance `m` means buyer-is-maker).
* Toobit open-interest unit; index symbol relation; Toobit contract multiplier use for quantity conversion.
* The docs were read through a summarising fetch tool: samples in tests reproduce documented STRUCTURE, not verbatim bodies.
* Binance `/public` vs `/market` routing was taken from the change notice; actual server behaviour is unconfirmed.
