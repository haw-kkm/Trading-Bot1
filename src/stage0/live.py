"""PUBLIC_READ_ONLY live runner (Binance + Toobit public market data). User-run only.

NOT executed by the tests or by the coding agent: tests inject fake REST/WebSocket transports.
Nothing here signs requests, reads API keys / .env, or touches order/account endpoints. Credentials that may
exist in the environment are never read and never attached to a request.

Run (each needs explicit flags, otherwise the tool refuses to connect):
  python -m src.stage0.live --check  --source binance-futures --symbol BTCUSDT --allow-binance-network
  python -m src.stage0.live          --source binance-futures --symbol BTCUSDT --seconds 30 --allow-binance-network --out binance.jsonl
  python -m src.stage0.live --check  --source toobit-futures --symbol BTC-SWAP-USDT --allow-toobit-network --i-approve-toobit-public-readonly
  python -m src.stage0.live          --source toobit-futures --symbol BTC-SWAP-USDT --seconds 30 \
         --allow-toobit-network --i-approve-toobit-public-readonly --out toobit.jsonl

Exit codes: 0 completed healthy | 2 refused (no network permission/approval) | 3 required REST data unavailable
            4 reconnect attempts exhausted | 5 data-integrity / internal error | 6 completed but UNHEALTHY
            (no events, mostly unparsable, or long data stall) | (--check) 1 = at least one FAIL
"""
import argparse
import asyncio
import json
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

from .adapters import binance, toobit
from .collector import (BINANCE_FUTURES_WS, WS_ALLOWED, WS_URLS, Collector, HttpError, NetworkDisabled,
                        NetworkPolicy, PublicRestClient, ReconnectPolicy, ResponseFormatError, TransportError)
from .envelope import EnvelopeError, Source
from .orderbook import BookState, LocalOrderBook
from .symbols import binance_to_common, toobit_to_common

# Event types that must be READY at the end of a collection for the run to count as healthy.
CORE_TYPES = {Source.BINANCE_SPOT: ("TRADE", "BOOK_TICKER"),
              Source.BINANCE_FUTURES: ("TRADE", "BOOK_TICKER", "MARK_PRICE"),
              Source.TOOBIT_FUTURES: ("TRADE", "BOOK_TICKER", "MARK_PRICE")}

EXIT_OK, EXIT_REFUSED, EXIT_STARTUP, EXIT_RECONNECT, EXIT_INTEGRITY, EXIT_UNHEALTHY = 0, 2, 3, 4, 5, 6


class WsClosed(Exception):
    pass


class StartupFailure(RuntimeError):
    """A REQUIRED initial REST datum (server time / snapshot) is unavailable -> fail closed (exit 3)."""


class ReconnectExhausted(RuntimeError):
    """Reconnect attempts used up (exit 4)."""


class ResyncExhausted(RuntimeError):
    """Order-book resync attempts used up: persistent sequence/integrity trouble (exit 5)."""


class FatalDataError(RuntimeError):
    """Permanent protocol/data error that retrying cannot fix (exit 5)."""


class RestFailure(Exception):
    def __init__(self, endpoint: str, kind: str, status: Optional[int], attempts: int, reason: str):
        super().__init__(f"{endpoint}: {kind} {status or ''} {reason}")
        self.endpoint, self.kind, self.status, self.attempts, self.reason = endpoint, kind, status, attempts, reason

    def record(self) -> dict:               # no params, headers or bodies: nothing sensitive
        return {"endpoint": self.endpoint, "kind": self.kind, "status": self.status,
                "attempts": self.attempts, "reason": self.reason}


@dataclass
class RestPolicy:
    max_attempts: int = 4                    # total tries per request; never unbounded
    base_s: float = 1.0
    cap_s: float = 30.0
    max_wait_s: float = 60.0                 # if the server asks to wait longer, give up instead of sleeping
    jitter: Callable[[float], float] = lambda d: random.uniform(0.0, 0.25 * d)


class WebsocketsConnection:                       # pragma: no cover - needs the network
    """Default transport (pip install websockets). Imported lazily so tests/CI never need it.
    The library answers server pings with pongs automatically (Binance requires this)."""

    async def connect(self, url: str):
        import websockets
        self._ws = await websockets.connect(url, open_timeout=15, close_timeout=2, ping_interval=None, max_size=2 ** 22)

    async def send(self, text: str):
        try:
            await self._ws.send(text)
        except Exception as e:
            raise WsClosed(str(e))

    async def recv(self) -> str:
        try:
            m = await self._ws.recv()
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                raise
            raise WsClosed(str(e))
        return m.decode() if isinstance(m, bytes) else m

    async def close(self):
        try:
            await self._ws.close()
        except Exception:
            pass


class PublicWsClient:
    def __init__(self, policy: NetworkPolicy, factory: Callable[[], object] = WebsocketsConnection):
        self.policy, self.factory = policy, factory

    async def open(self, source: Source, url: str):
        self.policy.check(source)                           # policy first, always
        if not any(url.startswith(p) for p in WS_ALLOWED[source]):
            raise NetworkDisabled(f"URL not under an allow-listed WebSocket endpoint: {url}")
        conn = self.factory()
        await conn.connect(url)
        return conn


def _utcnow():
    return datetime.now(timezone.utc)


class LiveRunner:
    def __init__(self, collector: Collector, rest: PublicRestClient, ws: PublicWsClient,
                 clock: Callable[[], datetime] = _utcnow, sleep=asyncio.sleep,
                 reconnect: Optional[ReconnectPolicy] = None, max_resyncs: int = 5,
                 ping_interval_s: float = 30.0, idle_timeout_s: float = 20.0,
                 rest_policy: Optional[RestPolicy] = None):
        self.c, self.rest, self.ws, self.clock, self.sleep = collector, rest, ws, clock, sleep
        self.reconnect = reconnect or ReconnectPolicy()
        self.rest_policy = rest_policy or RestPolicy()
        self.max_resyncs, self.ping_interval_s, self.idle_timeout_s = max_resyncs, ping_interval_s, idle_timeout_s
        self.reconnects = 0
        self.resyncs = 0
        self.network_calls = 0
        self.rest_failures: List[dict] = []
        self.rest_retries = 0
        self.ws_counts: Counter = Counter()           # events that arrived over the WebSocket, by type
        self.index_token: Optional[str] = None        # Toobit: contract's exchangeInfo indexToken (resolved per run)
        self.rest_counts: Counter = Counter()         # events that came from REST reference polls, by type
        self.end_state: Optional[dict] = None         # gate state captured when collection ended (before close)
        self.last_msg_at: Optional[datetime] = None
        self.max_gap_s = 0.0
        self.started_at: Optional[datetime] = None
        # Per-event-type LIVE WebSocket freshness bookkeeping (O(#types) memory). Only events the gate ACCEPTED
        # as current count: BACKFILL, duplicate, out-of-order and bad-timestamp events never refresh a stream,
        # and REST-sourced events never enter (channel must be "ws").
        self.stream_started_at: Optional[datetime] = None
        self.end_at: Optional[datetime] = None
        self._stream: Dict[str, dict] = {}

    # ------------------------------------------------------------------ REST with classification/backoff
    def _server_wait(self, e: HttpError) -> Optional[float]:
        """Rate-limit hints from the exchange itself, preferred over our own backoff."""
        ra = e.headers.get("retry-after")
        if ra is not None:
            try:
                return max(0.0, float(ra))
            except ValueError:
                pass
        ts = e.headers.get("x-api-limit-reset-timestamp")            # Toobit: ms epoch of the next window
        if ts is not None:
            try:
                return max(1.0, float(ts) / 1000.0 - self.clock().timestamp())
            except ValueError:
                pass
        return None

    async def _fetch(self, src: Source, path: str, params=None):
        pol = self.rest_policy
        last = None
        for attempt in range(1, pol.max_attempts + 1):
            self.network_calls += 1
            try:
                return await asyncio.to_thread(self.rest.get, src, path, params)
            except NetworkDisabled:
                raise
            except HttpError as e:
                if e.status == 429 or e.status == 418 or 500 <= e.status < 600:
                    kind, wait = "TRANSIENT", self._server_wait(e) if e.status in (429, 418) else None
                    last = (kind, e.status, f"HTTP {e.status}", wait)
                else:        # 400/401/403(WAF)/404/451 ...: retrying the same request cannot help
                    why = f"HTTP {e.status}"
                    if e.status == 451:
                        why += " (access restricted for legal/regional reasons; not retried, not bypassed)"
                    raise RestFailure(path, "PERMANENT", e.status, attempt, why)
            except TransportError as e:
                last = ("TRANSIENT", None, e.kind, None)
            except ResponseFormatError as e:
                raise RestFailure(path, "PERMANENT", None, attempt, "invalid JSON")
            kind, status, reason, wait = last
            if attempt >= pol.max_attempts:
                break
            delay = wait if wait is not None else min(pol.cap_s, pol.base_s * (2 ** (attempt - 1)))
            delay += pol.jitter(delay)
            if delay > pol.max_wait_s:
                reason += f" (server asked to wait {delay:.0f}s > {pol.max_wait_s:.0f}s: giving up)"
                break
            self.rest_retries += 1
            await self.sleep(delay)
        raise RestFailure(path, last[0], last[1], attempt, reason)

    def _note(self, f: RestFailure):
        self.rest_failures.append(f.record())

    def _ingest_all(self, envs, channel="rest"):
        for e in envs:
            t = e.event_type.value
            (self.ws_counts if channel == "ws" else self.rest_counts)[t] += 1
            self.c._ingest(e)
            if channel == "ws" and self.c.gate.is_current(e):
                self._note_live(t)

    def _note_live(self, etype: str):
        now = self.clock()
        s = self._stream.get(etype)
        if s is None:
            lead = (now - self.stream_started_at).total_seconds() if self.stream_started_at else 0.0
            self._stream[etype] = {"count": 1, "first": now, "last": now, "max_gap": max(0.0, lead)}
        else:
            s["max_gap"] = max(s["max_gap"], (now - s["last"]).total_seconds())
            s["count"] += 1
            s["last"] = now

    def _open_stream_window(self):
        if self.stream_started_at is None:
            self.stream_started_at = self.clock()

    def stream_report(self, end: Optional[datetime] = None) -> dict:
        """Per-type live-stream freshness over the window [first socket opened, END OF DATA]. The window ends
        at the end-of-data instant (before the socket close handshake), so a slow close is not counted as a
        stall and the time before streaming started (REST bootstrap) is not either. max_gap_s covers the
        leading gap (window start -> first live event), gaps between events, and the trailing gap (last event
        -> window end)."""
        end = end or self.end_at or self.clock()
        start = self.stream_started_at
        by_type = {}
        for t, s in sorted(self._stream.items()):
            by_type[t] = {"live_count": s["count"], "first_at": s["first"].isoformat(), "last_at": s["last"].isoformat(),
                          "max_gap_s": round(max(s["max_gap"], (end - s["last"]).total_seconds()), 1),
                          "end_age_s": round(max(0.0, (end - s["last"]).total_seconds()), 1)}
        return {"window_start": start.isoformat() if start else None, "window_end": end.isoformat(),
                "window_s": round((end - start).total_seconds(), 1) if start else None, "by_type": by_type}

    def _capture_end(self, src: Source, common: str):
        """Gate state at the END OF DATA (before the socket close handshake, which can take seconds)."""
        now = self.clock()
        self.end_at = now
        core = CORE_TYPES[src]
        self.end_state = {"at": now.isoformat(), "core_types": list(core),
                          "readiness": self.c.gate.status(src, common, now, types=core).value,
                          "per_type": self.c.gate.explain(src, common, now)}

    # ------------------------------------------------------------------ required: clock
    async def sync_clock(self, src: Source):
        """REQUIRED. Without a trustworthy exchange time the lag/future checks mean nothing -> fail closed."""
        path = {Source.TOOBIT_FUTURES: "/api/v1/time", Source.BINANCE_FUTURES: "/fapi/v1/time"}.get(src, "/api/v3/time")
        t0 = self.clock()
        try:
            raw = await self._fetch(src, path)
        except RestFailure as f:
            self._note(f)
            raise StartupFailure(f"server time unavailable ({f.kind} {f.status or ''} {f.reason})")
        t1 = self.clock()
        try:
            st = int(raw["serverTime"])
            if st <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise StartupFailure("server time response has no valid 'serverTime'")
        off = st - int((t0 + (t1 - t0) / 2).timestamp() * 1000)
        self.c.gate.on_clock_offset(src, off)
        return off

    # ------------------------------------------------------------------ optional: reference data
    def _reference_calls(self, src: Source, symbol: str):
        rx = self.clock
        if src == Source.TOOBIT_FUTURES:
            ts = toobit
            # exchangeInfo FIRST: its contracts[].indexToken is the only accepted /quote/v1/index key.
            return [("/api/v1/exchangeInfo", None, "CONTRACT_SPEC", lambda r: [e for e in ts.parse_contracts(r, rx())
                                                                              if e.exchange_symbol == symbol]),
                    ("/api/v1/futures/fundingRate", {"symbol": symbol}, "FUNDING_RATE", lambda r: ts.parse_funding_rate(r, rx())),
                    ("/quote/v1/openInterest", {"symbol": symbol}, "OPEN_INTEREST", lambda r: ts.parse_open_interest(r, rx(), expected_symbol=symbol)),
                    ("/quote/v1/markPrice", {"symbol": symbol}, "MARK_PRICE", lambda r: ts.parse_mark_price(r, rx())),
                    ("/quote/v1/index", INDEX_FROM_CONTRACT, "INDEX_PRICE",
                     lambda r, tok: ts.parse_index_price(r, rx(), expected_key=tok, index_token=tok, contract_symbol=symbol))]
        if src == Source.BINANCE_FUTURES:
            P = binance.FUTURES
            return [("/fapi/v1/openInterest", {"symbol": symbol}, "OPEN_INTEREST", lambda r: [P.parse_open_interest(r, rx())]),
                    ("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1}, "FUNDING_RATE", lambda r: P.parse_funding_history(r, rx())),
                    ("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": "5m", "limit": 1},
                     "LONG_SHORT_RATIO", lambda r: P.parse_long_short_ratio(r, rx()))]
        return []          # spot has no slow reference data; a one-shot REST price would only go stale

    async def poll_reference(self, src: Source, symbol: str):
        """Optional reference data. A failure is isolated (the stream keeps running) but is registered with
        the gate: any consumer that needs that data type sees a missing type and never gets READY."""
        common = toobit_to_common(symbol) if src == Source.TOOBIT_FUTURES else binance_to_common(
            symbol, src == Source.BINANCE_FUTURES)
        info = None                      # this run's exchangeInfo (Toobit): source of the index token
        for path, params, etype, fn in self._reference_calls(src, symbol):
            try:
                if params is INDEX_FROM_CONTRACT:
                    token, problem, _ = toobit.index_token_for(info, symbol)
                    if token is None:    # fail closed: never request a guessed index name
                        raise EnvelopeError(f"indexToken unresolved: {problem}")
                    self.index_token = token
                    raw = await self._fetch(src, path, {"symbol": token})
                    envs = fn(raw, token)
                else:
                    raw = await self._fetch(src, path, params)
                    if path == EXCHANGE_INFO_PATH:
                        info = raw
                    envs = fn(raw)
                if not envs:
                    raise EnvelopeError("response contained no usable item")
                self._ingest_all(envs)
            except RestFailure as f:
                self._note(f)
                self.c.gate.on_reference_failed(src, common, etype, f"{f.kind} {f.status or ''} {f.reason}")
            except (EnvelopeError, KeyError, ValueError, TypeError) as e:
                self.c.reject({"endpoint": path, "error": f"{type(e).__name__}: {e}"})
                self.c.gate.on_reference_failed(src, common, etype, f"unparsable: {type(e).__name__}")

    # ------------------------------------------------------------------ stream bookkeeping
    def _tick(self):
        now = self.clock()
        ref = self.last_msg_at or self.started_at
        if ref is not None:
            self.max_gap_s = max(self.max_gap_s, (now - ref).total_seconds())
        self.last_msg_at = now

    def _begin_sync(self, src, common):
        b = self.c.books.setdefault((src, common), LocalOrderBook(src == Source.BINANCE_FUTURES))
        b.start_sync()
        self.c.gate.report_book(src, common, BookState.SYNCING)

    async def _binance_snapshot(self, src, symbol):
        path = "/fapi/v1/depth" if src == Source.BINANCE_FUTURES else "/api/v3/depth"
        P = binance.FUTURES if src == Source.BINANCE_FUTURES else binance.SPOT
        raw = await self._fetch(src, path, {"symbol": symbol, "limit": 1000})
        snap = P.parse_depth_snapshot(symbol, raw, self.clock())
        self.rest_counts[snap.event_type.value] += 1          # REST bootstrap: counted so file/summary counts reconcile
        self.c._ingest(snap)

    async def _backoff_or_give_up(self, src, common, attempt, delays, why):
        self.c.gate.on_disconnect(src)
        self.reconnects += 1
        if attempt >= len(delays):
            self.c.gate.on_resync_failed(src, common, f"reconnect attempts exhausted: {why}")
            raise ReconnectExhausted(why)
        await self.sleep(delays[attempt])
        return attempt + 1

    # ------------------------------------------------------------------ Binance
    async def _binance_conn(self, src, symbol, common, url, with_book, deadline):
        P = binance.FUTURES if src == Source.BINANCE_FUTURES else binance.SPOT
        delays, attempt = self.reconnect.delays(), 0
        while self.clock().timestamp() < deadline:
            conn = None
            try:
                conn = await self.ws.open(src, url)
                self._open_stream_window()
                self.c.gate.on_connect(src, [common] if with_book else [])
                if with_book:
                    self._begin_sync(src, common)
                    await self._binance_snapshot(src, symbol)
                attempt = 0
                while self.clock().timestamp() < deadline:
                    raw = await asyncio.wait_for(conn.recv(), timeout=self.idle_timeout_s)
                    self._tick()
                    try:
                        self._ingest_all(P.parse(json.loads(raw), self.clock()), "ws")
                    except (EnvelopeError, KeyError, ValueError, TypeError) as e:
                        self.c.reject({"error": f"{type(e).__name__}: {e}"})
                    b = self.c.books.get((src, common)) if with_book else None
                    if b is not None and b.state == BookState.INVALID:
                        self.resyncs += 1
                        if self.resyncs > self.max_resyncs:
                            self.c.gate.on_resync_failed(src, common, b.reason or "")
                            raise ResyncExhausted(b.reason)
                        self._begin_sync(src, common)
                        await self._binance_snapshot(src, symbol)
                self._capture_end(src, common)
            except RestFailure as f:                   # snapshot could not be fetched
                self._note(f)
                if f.kind == "PERMANENT":
                    raise FatalDataError(f"depth snapshot rejected permanently: HTTP {f.status}")
                attempt = await self._backoff_or_give_up(src, common, attempt, delays, f"snapshot: {f.reason}")
            except (WsClosed, asyncio.TimeoutError, OSError) as e:
                attempt = await self._backoff_or_give_up(src, common, attempt, delays, str(e) or type(e).__name__)
            finally:
                if conn is not None:
                    await conn.close()

    async def run_binance(self, src: Source, symbol: str, duration_s: float):
        futures = src == Source.BINANCE_FUTURES
        common = binance_to_common(symbol, futures)
        s = symbol.lower()
        deadline = self.clock().timestamp() + duration_s
        if futures:       # routed endpoints: depth+bookTicker on /public, aggTrade+markPrice on /market
            plan = [(BINANCE_FUTURES_WS["public"] + f"?streams={s}@bookTicker/{s}@depth@100ms", True),
                    (BINANCE_FUTURES_WS["market"] + f"?streams={s}@aggTrade/{s}@markPrice@1s", False)]
        else:
            plan = [(WS_URLS[src] + f"?streams={s}@trade/{s}@bookTicker/{s}@depth@100ms", True)]
        tasks = [asyncio.ensure_future(self._binance_conn(src, symbol, common, u, wb, deadline)) for u, wb in plan]
        await self._fail_fast(tasks)

    @staticmethod
    async def _fail_fast(tasks):
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    # ------------------------------------------------------------------ Toobit
    async def run_toobit(self, symbol: str, duration_s: float, topics=("trade", "bookTicker", "markPrice", "depth")):
        src = Source.TOOBIT_FUTURES
        common = toobit_to_common(symbol)
        deadline = self.clock().timestamp() + duration_s
        delays, attempt = self.reconnect.delays(), 0
        while self.clock().timestamp() < deadline:
            conn = None
            try:
                conn = await self.ws.open(src, WS_URLS[src])
                self._open_stream_window()
                self.c.gate.on_connect(src, [common])
                for t in topics:
                    if t == "index" and self.index_token is None:
                        raise FatalDataError("index topic requested but the contract's indexToken is unresolved")
                    await conn.send(json.dumps(toobit.ws_subscribe_message(t, self.index_token if t == "index" else symbol)))
                self.c.gate.mark_synced(src, common)      # partial-depth snapshots need no sequence sync
                attempt, quiet = 0, 0
                while self.clock().timestamp() < deadline:
                    try:
                        raw = await asyncio.wait_for(conn.recv(), timeout=self.ping_interval_s)
                        quiet = 0
                    except asyncio.TimeoutError:
                        quiet += 1
                        if quiet >= 3:
                            raise
                        await conn.send(json.dumps({"ping": int(self.clock().timestamp() * 1000)}))
                        continue
                    self._tick()
                    try:
                        self._ingest_all(toobit.parse_ws(json.loads(raw), self.clock()), "ws")
                    except toobit.SubscriptionError as e:
                        raise FatalDataError(str(e))          # a rejected subscription cannot be retried away
                    except (EnvelopeError, KeyError, ValueError, TypeError) as e:
                        self.c.reject({"error": f"{type(e).__name__}: {e}"})
                self._capture_end(src, common)
            except (WsClosed, asyncio.TimeoutError, OSError) as e:
                attempt = await self._backoff_or_give_up(src, common, attempt, delays, str(e) or type(e).__name__)
            finally:
                if conn is not None:
                    await conn.close()

    async def run(self, src: Source, symbol: str, duration_s: float):
        self.started_at = self.clock()
        await self.sync_clock(src)                 # required (StartupFailure)
        await self.poll_reference(src, symbol)     # optional (isolated, but registered with the gate)
        if src == Source.TOOBIT_FUTURES:
            await self.run_toobit(symbol, duration_s)
        else:
            await self.run_binance(src, symbol, duration_s)

    # ------------------------------------------------------------------ verdict
    def health(self, stall_s: float = 20.0) -> Tuple[int, List[str]]:
        """Exit code for a run that ended WITHOUT an exception. A run that produced nothing useful is not success."""
        reasons = []
        n_ev, n_bad = self.c.total_envelopes, self.c.total_rejected
        end_gap = ((self.clock() - self.last_msg_at).total_seconds() if self.last_msg_at else 0.0)
        gap = max(self.max_gap_s, end_gap)
        if n_ev == 0:
            reasons.append("no events collected")
        if n_ev + n_bad and n_bad / (n_ev + n_bad) > 0.5:
            reasons.append(f"{n_bad} of {n_ev + n_bad} messages could not be parsed")
        if gap > stall_s:
            reasons.append(f"data stalled for {gap:.0f}s (> {stall_s:.0f}s)")
        if self.end_state is not None and self.end_state["readiness"] != "READY":
            core = {k: v for k, v in self.end_state["per_type"].items() if k in self.end_state["core_types"]}
            reasons.append(f"core data types not READY at end of collection ({self.end_state['readiness']}): {core}")
        return (EXIT_UNHEALTHY if reasons else EXIT_OK), reasons


# ---------------------------------------------------------------------- REST shape check (user-run)
PASS, FAIL, INCONCLUSIVE = "PASS", "FAIL", "INCONCLUSIVE"

EXPECTED_KEYS = {   # documented top-level (dict) or first-item (list) keys; extra keys are reported, not failed
    "binance:time": {"serverTime"},
    "binance:price": {"symbol", "price"},
    "binance:depth": {"lastUpdateId", "bids", "asks"},
    "binance:oi": {"symbol", "openInterest", "time"},
    "binance:funding": {"symbol", "fundingRate", "fundingTime"},
    "binance:ratio": {"symbol", "longShortRatio", "longAccount", "shortAccount", "timestamp"},
    "toobit:time": {"serverTime"},
    "toobit:price": {"s", "p"},
    "toobit:trades": {"p", "q", "t", "ibm"},
    "toobit:depth": {"t", "b", "a"},
    "toobit:bookTicker": {"t", "s", "b", "bq", "a", "aq"},
    "toobit:markPrice": {"exchangeId", "symbolId", "price", "time"},
    "toobit:index": {"index", "edp"},
    "toobit:funding": {"symbol", "rate", "period", "nextFundingTime", "interest", "fundingRateCap", "fundingRateFloor"},
    "toobit:oi": {"openInterestList"},
    "toobit:exchangeInfo": {"contracts"},
}


class _IndexFromContract:
    """Marker for the /quote/v1/index `symbol` parameter: it is NOT known when the REST table is built. It is
    resolved per run from the contract's exchangeInfo indexToken (official rule) - never guessed from the symbol."""

    def __repr__(self):
        return "<indexToken from exchangeInfo>"


INDEX_FROM_CONTRACT = _IndexFromContract()


def _is_empty(raw) -> bool:
    return raw is None or raw == [] or raw == {}


def _first_keys(raw) -> Optional[set]:
    if isinstance(raw, dict):
        return set(raw)
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return set(raw[0])
    return None


def _rest_table(src: Source, symbol: str, rx: datetime):
    """THE single definition of the REST requests of a --check run: (path, params, expected-keys id, parser, verification key).
    The verifier derives its required-endpoint contract from this same table (required_rest_endpoints)."""
    tb = src == Source.TOOBIT_FUTURES
    if tb:
        T = [("/api/v1/time", None, "toobit:time", None, None),
             ("/quote/v1/contract/ticker/price", {"symbol": symbol}, "toobit:price", lambda r: toobit.parse_ticker_price(r, rx), "rest_ticker_price"),
             ("/quote/v1/trades", {"symbol": symbol, "limit": 5}, "toobit:trades", lambda r: toobit.parse_trades(symbol, r, rx), "rest_trades"),
             ("/quote/v1/depth", {"symbol": symbol, "limit": 5}, "toobit:depth", lambda r: [toobit.parse_depth(symbol, r, rx)], "rest_depth"),
             ("/quote/v1/contract/ticker/bookTicker", {"symbol": symbol}, "toobit:bookTicker", lambda r: toobit.parse_book_ticker(r, rx), "rest_book_ticker"),
             ("/quote/v1/markPrice", {"symbol": symbol}, "toobit:markPrice", lambda r: toobit.parse_mark_price(r, rx), "rest_mark_price"),
             ("/api/v1/exchangeInfo", None, "toobit:exchangeInfo",
              lambda r: [e for e in toobit.parse_contracts(r, rx) if e.exchange_symbol == symbol], "rest_contract"),
             ("/quote/v1/index", INDEX_FROM_CONTRACT, "toobit:index",
              lambda r, tok: toobit.parse_index_price(r, rx, expected_key=tok, index_token=tok, contract_symbol=symbol),
              "rest_index_price"),
             ("/api/v1/futures/fundingRate", {"symbol": symbol}, "toobit:funding", lambda r: toobit.parse_funding_rate(r, rx), "rest_funding_rate"),
             ("/quote/v1/openInterest", {"symbol": symbol}, "toobit:oi",
              lambda r: toobit.parse_open_interest(r, rx, expected_symbol=symbol), "rest_open_interest")]
    else:
        fut = src == Source.BINANCE_FUTURES
        P = binance.FUTURES if fut else binance.SPOT
        pre = "/fapi/v1" if fut else "/api/v3"
        T = [(pre + "/time", None, "binance:time", None, None),
             (pre + "/ticker/price", {"symbol": symbol}, "binance:price", lambda r: [P.parse_price(r, rx)], None),
             (pre + "/depth", {"symbol": symbol, "limit": 5}, "binance:depth", lambda r: [P.parse_depth_snapshot(symbol, r, rx)], None)]
        if fut:
            T += [("/fapi/v1/openInterest", {"symbol": symbol}, "binance:oi", lambda r: [P.parse_open_interest(r, rx)], None),
                  ("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1}, "binance:funding", lambda r: P.parse_funding_history(r, rx), None),
                  ("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": "5m", "limit": 1},
                   "binance:ratio", lambda r: P.parse_long_short_ratio(r, rx), None)]
    return T


# REST rows whose MEANING is cross-checked against the exchangeInfo response of the SAME check run. Their verdict
# comes from the check (PASS only with this run's own evidence), never from a static constant.
SEMANTIC_CHECKS = {"rest_index_price": toobit.check_index_semantics,
                   "rest_open_interest": toobit.check_open_interest_semantics}
EXCHANGE_INFO_PATH = "/api/v1/exchangeInfo"

# ---- verification SCOPE (tools/stage0_live_verify.py) -------------------------------------------------------
# Core Public Market Data Collection = receipt, parsing, per-stream freshness, sanity, collector exit and JSONL
# evidence of the data the core uses. Extended Data Semantics = meaning of data whose contract mapping / unit is
# not officially confirmed. A type may be EXCLUDED from core only if core never uses it (not a core gate type,
# not a required WebSocket type); the exclusion is always reported, never hidden.
EXTENDED_SEMANTIC_ENDPOINTS = {Source.TOOBIT_FUTURES: {"/quote/v1/index": "INDEX_PRICE",
                                                       "/quote/v1/openInterest": "OPEN_INTEREST"}}
INDEX_COMPONENTS_PATH = "/quote/v1/indexPriceComponents"


async def check_extended(rest: PublicRestClient, src: Source, symbol: str, index_token: Optional[str] = None) -> List[dict]:
    """Extended-semantics EVIDENCE only (not part of the REST contract, never changes the --check exit code).
    Toobit: GET /quote/v1/indexPriceComponents?symbol=<index name> validates that index's structure/values;
    it never proves the contract->index mapping. Reports names/counts only, no prices."""
    if src != Source.TOOBIT_FUTURES:
        return []
    name = index_token                   # the contract's exchangeInfo indexToken from the same --check run, or None
    row = {"endpoint": INDEX_COMPONENTS_PATH, "params": {"symbol": name}, "verdict": FAIL, "http_status": None,
           "scope": "extended_semantics", "proves_contract_mapping": False}
    if not name:
        row.update(verdict=INCONCLUSIVE, reason="indexToken of the contract unresolved in this run: not requested "
                                                "(an index name is never guessed from the contract symbol)")
        return [row]
    try:
        raw = await asyncio.to_thread(rest.get, src, INDEX_COMPONENTS_PATH, {"symbol": name})
    except NetworkDisabled:
        raise
    except HttpError as e:
        row.update(http_status=e.status, reason=f"HTTP {e.status}")
        return [row]
    except TransportError as e:
        row["reason"] = f"transport error: {e.kind}"
        return [row]
    except ResponseFormatError:
        row["reason"] = "response is not valid JSON"
        return [row]
    except Exception as e:              # extended evidence must never break the REST check: record it as FAIL
        row["reason"] = f"internal error: {type(e).__name__}"
        return [row]
    res = toobit.check_index_components(name, raw)
    row.update(http_status=200, verdict=res["verdict"], reason=res["reason"], evidence=res["evidence"])
    return [row]


def required_rest_endpoints(src: Source) -> List[str]:
    """Endpoint paths a complete --check run must report exactly once each."""
    return [row[0] for row in _rest_table(src, "BTC-SWAP-USDT" if src == Source.TOOBIT_FUTURES else "BTCUSDT", _utcnow())]


async def check_rest(rest: PublicRestClient, src: Source, symbol: str, now: Callable[[], datetime] = _utcnow) -> List[dict]:
    """One request per documented public REST endpoint, then PASS / FAIL / INCONCLUSIVE:
      PASS          response received, required structure present, parser produced events, data non-empty
      FAIL          request failed (HTTP/transport) or the structure contradicts the parser's expectation
      INCONCLUSIVE  empty response / no events / docs leave the meaning or unit open (never auto-promoted)
    Toobit index / openInterest rows are resolved AFTER all requests by SEMANTIC_CHECKS against this run's own
    exchangeInfo (a failed exchangeInfo request leaves them INCONCLUSIVE, never PASS).
    Reports shapes and counts only - never response values (apart from the clock offset) and no credentials."""
    rx = now()
    tb = src == Source.TOOBIT_FUTURES
    T = _rest_table(src, symbol, rx)
    out, raws, deferred = [], {}, []
    for path, params, ek, fn, vkey in T:
        row = {"endpoint": path, "verdict": FAIL, "request_ok": False, "http_status": None}
        if params is INDEX_FROM_CONTRACT:
            token, problem, contradictory = toobit.index_token_for(raws.get(EXCHANGE_INFO_PATH), symbol)
            if token is None:        # fail closed: no request for a guessed index name, no fallback
                row.update(verdict=FAIL if contradictory else INCONCLUSIVE,
                           reason=f"index not requested: {problem} (indexToken is the only accepted index key)")
                out.append(row)
                continue
            row["index_token"] = token
            params, fn = {"symbol": token}, (lambda r, _fn=fn, _t=token: _fn(r, _t))
        try:
            raw = await asyncio.to_thread(rest.get, src, path, params)
            row.update(request_ok=True, http_status=200)
            raws[path] = raw
        except NetworkDisabled:
            raise
        except HttpError as e:
            row.update(http_status=e.status, reason=("rate limited" if e.status == 429 else
                                                     "access restricted for legal/regional reasons (HTTP 451); not bypassed"
                                                     if e.status == 451 else
                                                     "server error" if e.status >= 500 else "request rejected"))
            out.append(row)
            continue
        except TransportError as e:
            row["reason"] = f"transport error: {e.kind}"
            out.append(row)
            continue
        except ResponseFormatError:
            row["reason"] = "response is not valid JSON"
            out.append(row)
            continue
        row["top_level"] = sorted(raw) if isinstance(raw, dict) else f"array[{len(raw)}]" if isinstance(raw, list) else type(raw).__name__
        keys = _first_keys(raw)
        if keys is not None:
            exp = EXPECTED_KEYS[ek]
            row["extra_keys"] = sorted(keys - exp)
            row["missing_keys"] = sorted(exp - keys)
        if fn is None:                                  # /time: validate the field itself
            try:
                off = int(raw["serverTime"]) - int(rx.timestamp() * 1000)
            except (KeyError, TypeError, ValueError):
                row.update(reason="serverTime missing or not an integer")
            else:
                row["clock_offset_ms"] = off
                if abs(off) > 300_000:
                    row.update(verdict=INCONCLUSIVE, reason=f"structure OK but server time differs from local by {off} ms")
                else:
                    row.update(verdict=PASS, reason="serverTime present and plausible")
            out.append(row)
            continue
        if _is_empty(raw):
            row.update(verdict=INCONCLUSIVE, parsed_events=0, reason="empty response: nothing to validate")
            out.append(row)
            continue
        try:
            n = len(fn(raw))
        except (EnvelopeError, KeyError, ValueError, TypeError) as e:
            row.update(parsed_events=None, reason=f"structure does not match parser: {type(e).__name__}: {e}")
            out.append(row)
            continue
        row["parsed_events"] = n
        if n == 0:
            row.update(verdict=INCONCLUSIVE, reason="parsed OK but produced no events for this symbol")
        elif tb and vkey in SEMANTIC_CHECKS:
            row.update(verdict=INCONCLUSIVE, structure=PASS, reason="semantic check pending")
            deferred.append((row, vkey, raw))
        elif vkey and toobit.VERIFICATION.get(vkey) == toobit.UNVERIFIED:
            row.update(verdict=INCONCLUSIVE, structure=PASS,
                       reason="structure matches the parser, but docs leave unit/symbol meaning open (stays UNVERIFIED)")
        else:
            row.update(verdict=PASS, structure=PASS,
                       reason="structure matches parser" + ("; semantics are DOC-level only (not independently verified)" if tb else
                                                             "; values not independently verified"))
        out.append(row)
    for row, vkey, raw in deferred:
        res = SEMANTIC_CHECKS[vkey](symbol, raws.get(EXCHANGE_INFO_PATH), raw, rx)
        if res["verdict"] not in (PASS, INCONCLUSIVE, FAIL):        # fail closed on anything unexpected
            res = {"verdict": FAIL, "reason": f"semantic check returned {res['verdict']!r}", "evidence": res.get("evidence")}
        level = toobit.VERIFICATION.get(vkey)
        row.update(verdict=res["verdict"], reason=f"semantics: {res['reason']} [static level: {level}]",
                   semantics=res["evidence"], semantic_verdict=res["verdict"], static_level=level)
    return out


def build_policy(args) -> NetworkPolicy:
    return NetworkPolicy(allow_binance=args.allow_binance_network, allow_toobit=args.allow_toobit_network,
                         toobit_user_approved=args.i_approve_toobit_public_readonly)


def _write(out, col):
    """Write the envelope JSONL (LF line endings on every OS) and return what was written, so the summary can
    be cross-checked against the file by an independent verifier. None if no file was requested."""
    if not out:
        return None
    import hashlib
    h, n = hashlib.sha256(), 0
    with open(out, "w", encoding="utf-8", newline="\n") as fh:
        for e in col.envelopes:
            line = e.to_json() + "\n"
            fh.write(line)
            h.update(line.encode("utf-8"))
            n += 1
    return {"lines": n, "sha256": h.hexdigest()}


class JsonlSink:
    """Streams envelopes to a JSONL file as they arrive (bounded memory) and keeps a running SHA-256 + line count."""

    def __init__(self, path):
        import hashlib
        self._fh = open(path, "w", encoding="utf-8", newline="\n")
        self._h, self.lines = hashlib.sha256(), 0

    def __call__(self, env):
        line = env.to_json() + "\n"
        self._fh.write(line)
        self._h.update(line.encode("utf-8"))
        self.lines += 1

    def close(self):
        self._fh.close()
        return {"lines": self.lines, "sha256": self._h.hexdigest()}


# In-memory retention for a live run: the JSONL sink is the evidence, memory only keeps a recent window.
LIVE_MAX_ENVELOPES, LIVE_MAX_REJECTED = 2_000, 2_000


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Public read-only market data runner (user-run).")
    ap.add_argument("--source", required=True, choices=[s.value for s in Source])
    ap.add_argument("--symbol", required=True, help="EXCHANGE symbol, e.g. BTCUSDT or BTC-SWAP-USDT")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--out", default=None, help="write envelopes as JSONL")
    ap.add_argument("--check", action="store_true", help="REST shape report (PASS/FAIL/INCONCLUSIVE); no streaming")
    ap.add_argument("--stall-seconds", type=float, default=20.0, help="data gap treated as unhealthy")
    ap.add_argument("--allow-binance-network", action="store_true")
    ap.add_argument("--allow-toobit-network", action="store_true")
    ap.add_argument("--i-approve-toobit-public-readonly", action="store_true",
                    help="your explicit approval for PUBLIC, unauthenticated Toobit market-data GET/WS only")
    args = ap.parse_args(argv)
    src = Source(args.source)
    pol = build_policy(args)
    try:
        pol.check(src)
    except NetworkDisabled as e:
        print(f"REFUSED: {e}. Add the explicit flag(s) described in the module docstring.", file=sys.stderr)
        return EXIT_REFUSED
    rest = PublicRestClient(pol)
    if args.check:
        try:
            rep = asyncio.run(check_rest(rest, src, args.symbol))
        except NetworkDisabled as e:
            print(f"REFUSED: {e}", file=sys.stderr)
            return EXIT_REFUSED
        counts = Counter(r["verdict"] for r in rep)
        out = {"summary": dict(counts), "endpoints": rep}
        if src == Source.TOOBIT_FUTURES:
            try:
                tok = next((r.get("index_token") for r in rep if r.get("endpoint") == "/quote/v1/index"), None)
                out["extended_evidence"] = asyncio.run(check_extended(rest, src, args.symbol, index_token=tok))
            except NetworkDisabled as e:
                out["extended_evidence"] = [{"endpoint": INDEX_COMPONENTS_PATH, "verdict": FAIL, "reason": f"refused: {e}"}]
            out["semantic_questions"] = toobit.semantic_status()
        print(json.dumps(out, indent=2))
        return 1 if counts.get(FAIL) else 0          # the REST contract only: extended evidence never changes it
    sink = JsonlSink(args.out) if args.out else None
    col = Collector(max_envelopes=LIVE_MAX_ENVELOPES, max_rejected=LIVE_MAX_REJECTED, sink=sink)
    runner = LiveRunner(col, rest, PublicWsClient(pol))
    code, reasons, err = EXIT_OK, [], None
    try:
        asyncio.run(runner.run(src, args.symbol, args.seconds))
        code, reasons = runner.health(args.stall_seconds)
    except NetworkDisabled as e:
        code, err = EXIT_REFUSED, f"network not permitted: {e}"
    except StartupFailure as e:
        code, err = EXIT_STARTUP, f"required REST data unavailable: {e}"
    except ReconnectExhausted as e:
        code, err = EXIT_RECONNECT, f"reconnect attempts exhausted: {e}"
    except (ResyncExhausted, FatalDataError) as e:
        code, err = EXIT_INTEGRITY, f"data integrity: {type(e).__name__}: {e}"
    except Exception as e:                                  # internal error: never report as success
        code, err = EXIT_INTEGRITY, f"internal error: {type(e).__name__}"
    written = sink.close() if sink else None
    common = toobit_to_common(args.symbol) if src == Source.TOOBIT_FUTURES else binance_to_common(
        args.symbol, src == Source.BINANCE_FUTURES)
    print(json.dumps({"exit_code": code, "error": err, "unhealthy_reasons": reasons,
                      "envelopes": col.total_envelopes, "rejected": col.total_rejected,
                      "issues": dict(col.gate.issue_counts), "reconnects": runner.reconnects,
                      "resyncs": runner.resyncs, "rest_retries": runner.rest_retries,
                      "ws_events_by_type": dict(runner.ws_counts), "rest_events_by_type": dict(runner.rest_counts),
                      "rejected_by_error": dict(Counter(r["error"][:120] for r in col.rejected)),
                      "end_of_collection": runner.end_state, "stream_freshness": runner.stream_report(),
                      "events_file_written": written,
                      "rest_failures": runner.rest_failures, "max_gap_s": round(runner.max_gap_s, 1),
                      }, indent=2))
    if err:
        print(f"FAILED: {err}", file=sys.stderr)
    elif reasons:
        print("UNHEALTHY: " + "; ".join(reasons), file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
