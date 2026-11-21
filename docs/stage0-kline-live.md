# Stage 0 — Binance KLINE live wiring (BTCUSDT, UTC 1m/5m)

Status: **implemented + offline-tested. `LIVE KLINE: NOT VERIFIED`.** No real Binance connection was made by the
implementation or the tests. `LIVE TRADING: NOT APPROVED` (unchanged; no order/auth/API-key code exists here).

## What is wired
`python -m src.stage0.live --source binance-spot|binance-futures --kline-intervals 1m,5m --out <file> ...`

| item | behaviour |
|---|---|
| default | without `--kline-intervals` the subscription is byte-identical to before |
| intervals | only `1m`, `5m`; `1s`, `15m`, `1h`, `""`, offset variants are refused (exit 2) **before** any network object exists; duplicates collapse; order is canonical |
| `--out` | required when streaming KLINE (evidence files are the record). Binance-only (Toobit refused) |
| `--check` | REST-only; with `--kline-intervals` it reports `kline_streaming.status = NOT_EXERCISED` — never a streaming success |
| Spot | `btcusdt@kline_1m/btcusdt@kline_5m` appended to the existing combined stream |
| Futures | appended to the **`/market/stream`** connection only; `/public/stream` stays bookTicker + depth; legacy `/stream`/`/ws` never used |
| reconnect | the KLINE streams are part of the connection URL, so every reconnect re-subscribes them |

Routing source (read 2026-11-22): Binance USDⓈ-M "Important WebSocket Change Notice"
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
(Kline = `/market`; bookTicker + diff depth = `/public`; legacy `/ws` and `/stream` announced for removal 2026-04-23).
The Futures Kline-Candlestick page itself could not be rendered by the fetch tool, so Futures *payload* semantics are
only checked against the Spot docs (https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams:
`<symbol>@kline_<interval>`, UTC+0 default, 2000 ms update speed for non-1s intervals, `E`/`t`/`T` are UTC ms,
`x` = "is this kline closed?"). Note the notice page is dated after its own cutoff — confirm on the first live run that
`/market/stream` actually delivers kline frames.

The v3.5 state machine (`kline.py`), gate and Stage 1 access are **unchanged**.

## Evidence files
* `<out>` — validated Envelope JSONL. It is a **receipt log of parsed frames**, including updates the gate later
  excluded (non-monotone OPEN, duplicates, …); it is **not** the approved set. Stage 1 reads the gate, not this file.
* `<out>.rejected.jsonl` — frames that failed validation (premature CLOSED, unsupported interval, malformed `x`, …) with `E`,
  `k.t`, `k.T`, `x`, `interval`, `received_at`, the error and the raw JSON. Never an analysis input.
* The summary attests lines + SHA-256 of both files, computed over the bytes actually written, plus `fsync: OK|FAILED`.

### Durability levels (what each really guarantees)
| level | when | survives process kill | survives OS crash / power loss |
|---|---|---|---|
| buffered `write()` | per record | no | no |
| `flush()` | every 100 records **or** 1 s, whichever first. The 1 s bound is enforced by a real-time timer in `LiveRunner.run()` (polls every 0.25 s, so worst case ≈ 1.25 s), **independent of WebSocket frames**; also checked on every write and every frame. Direct use of `run_binance()`/`run_toobit()` without `run()` has no timer (frame/write checks only) | **yes** (data is in the OS page cache) | no |
| `fsync()` | once, on normal close | yes | yes, as far as the OS/drive honour fsync |

`fsync` is intentionally not per event. After a power failure everything written since the OS last wrote back dirty pages
may be lost; the SHA-256/line attestation is only produced by a normal close, so a crashed run has **no** attestation.
A failing fsync makes the run exit 5 with `fsync: "FAILED"` and `durability_errors` — it is never reported as success.

## Report (`kline_diagnostics`, additive; existing fields unchanged)
Per interval: `received = parsed + parse_rejected`; `parsed = gate_accepted + gate_rejected`;
`gate_accepted = open_accepted + closed_accepted`. Plus parse/gate rejection reasons, `RECEIVE_LAG` (receive-lag histogram),
`CLOSE_EARLY_BY_MS`, `KLINE_LATE_FINALIZATION`, `E-(T+1ms)` histogram of accepted CLOSED bars, the newest approved CLOSED and
`freshness` ∈ `CURRENT | STALE | NO_APPROVED_CLOSED | NO_DATA`; global tie/conflict/non-monotone totals.
Four different notions are kept apart: *parsed*, *gate-accepted*, *CLOSED-approved*, *Stage 1 READY*.
The runner **never declares Stage 1 READY** (`stage1_trend_ready: NOT_DECLARED`, `core_pass_implies_kline_ready: false`).
A requested interval that produces no frame at all makes the run UNHEALTHY (exit 6).

Futures end state: the two connections each report end-of-data (`collection_end.captures == 2`); the latest report wins and
contains both connections' data (they share one gate). This is asserted by a test.

## Live procedure (user-run; requires the existing explicit Binance network permission)
```
python -m src.stage0.live --check --source binance-futures --symbol BTCUSDT --allow-binance-network
python -m src.stage0.live --source binance-spot    --symbol BTCUSDT --seconds 900 --kline-intervals 1m,5m \
       --allow-binance-network --out stage0-live-evidence/kline-spot.jsonl
python -m src.stage0.live --source binance-futures --symbol BTCUSDT --seconds 900 --kline-intervals 1m,5m \
       --allow-binance-network --out stage0-live-evidence/kline-futures.jsonl
```
≥ 15 min is needed to see several 1m CLOSED and at least two 5m CLOSED bars. After the run check: OPEN and CLOSED seen for both
intervals; `E-(T+1ms)` and receive-lag distributions; non-monotone / tie counts; both files present and attested; and whether the
initial 1000 ms (premature) and 5000 ms (late-finalization / receive-lag) thresholds fit the data. Those thresholds are
**assumptions until measured** and must not be loosened without that evidence.
