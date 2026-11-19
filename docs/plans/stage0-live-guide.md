# Stage 0 public-data connection guide (user-run)

Everything here is PUBLIC, UNAUTHENTICATED market data. No API key is needed, read or accepted.
The coding agent has NOT run any of this against a real exchange; CI (run 44541334075 on 628e64e) only proves
the offline tests pass. A real connection succeeding is something only you can confirm.

## Setup
```
pip install -r requirements.txt     # adds `websockets` (used only by the live runner)
```

## Steps (do them in this order; each is separate evidence)
1. **Offline tests** (already run by CI): `python -m pytest -q tests/unit -k stage0`
2. **Binance REST shape check** (GET only):
   `python -m src.stage0.live --check --source binance-futures --symbol BTCUSDT --allow-binance-network`
3. **Binance streaming** (30 s, writes envelopes):
   `python -m src.stage0.live --source binance-futures --symbol BTCUSDT --seconds 30 --allow-binance-network --out binance.jsonl`
   Spot: `--source binance-spot`. The summary prints `issues` (gate findings), `reconnects`, `resyncs`.
4. **Toobit** — only if YOU decide to approve public read-only access. Without BOTH flags the tool refuses:
   `python -m src.stage0.live --check --source toobit-futures --symbol BTC-SWAP-USDT --allow-toobit-network --i-approve-toobit-public-readonly`
   Then streaming with the same flags. The constant `TOOBIT_NETWORK_APPROVED` in code stays False; your approval
   exists only as the run-time flag on that command line.
5. **Compare** the `--check` report with the docs: any endpoint with `"ok": false` or unexpected `top_level` /
   `first_item_keys` is a parser/doc mismatch to fix. Only after matching live responses should someone edit
   `adapters/toobit.py::VERIFICATION` (UNVERIFIED -> DOC -> a new live-verified level).

## What each Toobit data type is allowed to claim
| Type | Level | Why |
|---|---|---|
| REST price, trades, depth, bookTicker, markPrice, fundingRate, exchangeInfo contracts; WS trade, markPrice, depth, bookTicker, kline | DOC | structure matches docs read 2026-11-20; no live comparison yet |
| REST index; WS index | UNVERIFIED | docs don't relate the `BTCUSDT`-style map key to `BTC-SWAP-USDT` |
| REST openInterest | UNVERIFIED | `size` has no documented unit |
| WS diffDepth | UNVERIFIED | docs say version numbers are not guaranteed unique -> no gap detection; never used to build a book |
UNVERIFIED data makes `DataQualityGate.status()` return UNVERIFIED (never READY) for any consumer that needs it
(`usable(..., types=[...])` lets a consumer ask only about the types it relies on).

## Known unverified points
* Toobit REST host `https://api.toobit.com` is NOT on the docs pages read; it comes from ccxt's definition.
* Toobit funding endpoint has no `time` field: events carry no event time (flag `NO_EVENT_TIME`, reliability penalty).
* Toobit WS depth/trade ordering semantics, trade `m` meaning ("true = buy" per docs, opposite convention to Binance `m`).
* Binance field layouts and the futures stream URL were not re-checked against live responses.
* Docs were read through a summarising fetch: no verbatim response samples were available, so test samples
  reproduce documented STRUCTURE with synthetic values.
* Real-time collection, reconnect behaviour under real network faults, and rate-limit behaviour are untested.
