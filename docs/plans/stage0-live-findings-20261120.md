# Stage 0 live verification findings (2026-11-19/20)

Public, unauthenticated market data only. No account/order/auth endpoint was called; no API key was used.
Evidence: GitHub Actions run 44578955235 @ e62baf7 (artifact `stage0-live-evidence-e62baf71…`, needs a GitHub login
to download) and a local re-run from a developer machine (not a CI artifact). Statuses: PASS / FAIL / INCONCLUSIVE / NOT_RUN.

## What the CI runner (GitHub, US region) saw
| Market | REST | WebSocket |
|---|---|---|
| Binance Spot | FAIL — all requests HTTP 451 | NOT_RUN (startup REST unavailable) |
| Binance Futures | FAIL — all requests HTTP 451 | NOT_RUN |
| Toobit Futures | 8 PASS, 1 FAIL (index), 1 INCONCLUSIVE (open interest) | received TRADE/BOOK_TICKER/SNAPSHOT; MARK_PRICE missing; 31 messages rejected |

**HTTP 451 cause:** "unavailable for legal reasons" — Binance restricts access from certain jurisdictions/IP ranges and the
GitHub-hosted runner is in one of them. It is NOT a code bug. It is not worked around (no proxy/VPN/header tricks). Binance can
only be verified from a network where Binance permits access (the same code passed from a developer machine, below). CI now
reports this as `FAIL` (REST) + `NOT_RUN` (WebSocket) and exits with code 2 (warning), never PASS.

## Toobit defects found and fixed
1. **Index REST FAIL** — `GET /quote/v1/index?symbol=BTC-SWAP-USDT` returns `{"index":{},"edp":{}}`; the endpoint is keyed by
   `BTCUSDT`. The old parser raised on the empty map. Fix: query with `toobit.index_symbol()`; an empty map yields no events
   (INCONCLUSIVE, not a crash). The WS `index` topic with the swap symbol answers `{"code":"-100010","desc":"Invalid Symbols!"}`;
   such error objects now raise `SubscriptionError` → exit 5 instead of being silently dropped.
2. **WS markPrice never parsed** — observed: an empty ack push (`data: []`), then `data` as a dict
   `{exchangeId, symbolId, price, time}` without a top-level `symbol`. The docs describe `{symbol, markPrice, time}`. The parser
   required the docs form, so every push (31 of them) was rejected. Fix: accept the observed form first, docs form as fallback.
3. **RECEIVE_LAG noise from trade history** — the first trade push (`f: true`) replays ~60 recent trades up to ~15 s old.
   They are now flagged `BACKFILL`: no lag check, and they never make `TRADE` fresh.
4. **Misleading "STALE" at the end of a run** — the WebSocket close handshake (up to ~10 s) ran before the state was read.
   End-of-collection gate state is now captured before closing and the close timeout is 2 s.
5. **Gate default** — a one-shot REST snapshot made the default "all seen types" check STALE after 10 s in every Binance run;
   a snapshot followed by a delta stream is now treated as bootstrap data.

## Local re-run after the fixes (developer machine, Korea-reachable network; not a CI artifact)
| Source | rest | ws_received | ws_parsed | ws_fresh | gate_ready |
|---|---|---|---|---|---|
| binance-spot | PASS (3/3) | PASS | PASS (7943 parsed, 0 rejected) | PASS (max gap 0.7 s) | PASS |
| binance-futures | PASS (6/6) | PASS | PASS (31495 / 0) | PASS (0.9 s) | PASS |
| toobit-futures | INCONCLUSIVE (8 PASS; index and open interest INCONCLUSIVE) | PASS | PASS (485 / 0) | PASS (1.2 s) | PASS |

Toobit stays INCONCLUSIVE overall on purpose: open-interest `size` has no documented unit, and the index↔swap symbol relation is
only supported by matching values (index ≈ mark price), not by documentation. Neither is auto-promoted.

## Still unverified
Toobit open-interest unit; diffDepth sequence semantics (observed `v` strings with a trailing counter, still not used for books);
Binance `/public` vs `/market` split worked from a developer machine but only for ~30 s; long-run behaviour (24 h reconnects, rate limits).
