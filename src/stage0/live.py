"""PUBLIC_READ_ONLY live runner (Binance + Toobit public market data). User-run only.

NOT executed by the tests or by the coding agent: tests inject fake REST/WebSocket transports.
Nothing here signs requests, reads API keys, or touches order/account endpoints.

Run (examples; each needs explicit flags, otherwise it refuses to connect):
  python -m src.stage0.live --source binance-futures --symbol BTCUSDT --seconds 30 --allow-binance-network --out live.jsonl
  python -m src.stage0.live --source toobit-futures --symbol BTC-SWAP-USDT --seconds 30 \
         --allow-toobit-network --i-approve-toobit-public-readonly --out live.jsonl
  python -m src.stage0.live --check --source toobit-futures --symbol BTC-SWAP-USDT \
         --allow-toobit-network --i-approve-toobit-public-readonly      # REST shape report vs the parsers
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Callable, List, Optional

from .adapters import binance, toobit
from .collector import (WS_URLS, Collector, NetworkDisabled, NetworkPolicy, PublicRestClient, ReconnectPolicy)
from .envelope import EnvelopeError, Source
from .orderbook import BookState
from .symbols import binance_to_common, toobit_to_common


class WsClosed(Exception):
    pass


class WebsocketsConnection:                       # pragma: no cover - needs the network
    """Default transport (pip install websockets). Imported lazily so tests/CI never need it."""

    async def connect(self, url: str):
        import websockets
        self._ws = await websockets.connect(url, open_timeout=15, ping_interval=None, max_size=2 ** 22)

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
        if not url.startswith(WS_URLS[source]):
            raise NetworkDisabled(f"URL not under the allow-listed WebSocket endpoint: {url}")
        conn = self.factory()
        await conn.connect(url)
        return conn


class ResyncExhausted(RuntimeError):
    pass


def _utcnow():
    return datetime.now(timezone.utc)


class LiveRunner:
    def __init__(self, collector: Collector, rest: PublicRestClient, ws: PublicWsClient,
                 clock: Callable[[], datetime] = _utcnow, sleep=asyncio.sleep,
                 reconnect: Optional[ReconnectPolicy] = None, max_resyncs: int = 5,
                 ping_interval_s: float = 30.0, idle_timeout_s: float = 20.0):
        self.c, self.rest, self.ws, self.clock, self.sleep = collector, rest, ws, clock, sleep
        self.reconnect = reconnect or ReconnectPolicy()
        self.max_resyncs, self.ping_interval_s, self.idle_timeout_s = max_resyncs, ping_interval_s, idle_timeout_s
        self.reconnects = 0
        self.resyncs = 0
        self.network_calls = 0

    # ------------------------------------------------------------------ REST helpers
    async def _get(self, src, path, params=None):
        self.network_calls += 1
        return await asyncio.to_thread(self.rest.get, src, path, params)

    def _ingest_all(self, envs):
        for e in envs:
            self.c._ingest(e)

    async def sync_clock(self, src: Source):
        path = "/api/v1/time" if src == Source.TOOBIT_FUTURES else (
            "/fapi/v1/time" if src == Source.BINANCE_FUTURES else "/api/v3/time")
        t0 = self.clock()
        raw = await self._get(src, path)
        t1 = self.clock()
        mid = t0 + (t1 - t0) / 2
        off = int(raw["serverTime"]) - int(mid.timestamp() * 1000)
        self.c.gate.on_clock_offset(src, off)
        return off

    async def poll_reference(self, src: Source, symbol: str):
        """One-shot REST reference data (funding, OI, contract spec ...). Failures are quarantined."""
        calls = []
        rx = self.clock
        if src == Source.TOOBIT_FUTURES:
            calls = [("/api/v1/futures/fundingRate", {"symbol": symbol}, lambda r: toobit.parse_funding_rate(r, rx())),
                     ("/quote/v1/openInterest", {"symbol": symbol}, lambda r: toobit.parse_open_interest(r, rx())),
                     ("/quote/v1/markPrice", {"symbol": symbol}, lambda r: toobit.parse_mark_price(r, rx())),
                     ("/quote/v1/index", {"symbol": symbol}, lambda r: toobit.parse_index_price(r, rx())),
                     ("/quote/v1/contract/ticker/bookTicker", {"symbol": symbol}, lambda r: toobit.parse_book_ticker(r, rx())),
                     ("/api/v1/exchangeInfo", None, lambda r: [e for e in toobit.parse_contracts(r, rx())
                                                              if e.exchange_symbol == symbol])]
        elif src == Source.BINANCE_FUTURES:
            P = binance.FUTURES
            calls = [("/fapi/v1/openInterest", {"symbol": symbol}, lambda r: [P.parse_open_interest(r, rx())]),
                     ("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1}, lambda r: P.parse_funding_history(r, rx())),
                     ("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": "5m", "limit": 1},
                      lambda r: P.parse_long_short_ratio(r, rx()))]
        else:
            calls = [("/api/v3/ticker/price", {"symbol": symbol}, lambda r: [binance.SPOT.parse_price(r, rx())])]
        for path, params, fn in calls:
            try:
                self._ingest_all(fn(await self._get(src, path, params)))
            except (EnvelopeError, KeyError, ValueError, TypeError) as e:
                self.c.rejected.append({"endpoint": path, "error": f"{type(e).__name__}: {e}"})

    # ------------------------------------------------------------------ Binance
    async def _binance_snapshot(self, src, symbol, common):
        path = "/fapi/v1/depth" if src == Source.BINANCE_FUTURES else "/api/v3/depth"
        P = binance.FUTURES if src == Source.BINANCE_FUTURES else binance.SPOT
        raw = await self._get(src, path, {"symbol": symbol, "limit": 1000})
        self.c._ingest(P.parse_depth_snapshot(symbol, raw, self.clock()))

    def _begin_sync(self, src, common):
        from .orderbook import LocalOrderBook
        b = self.c.books.setdefault((src, common), LocalOrderBook(src == Source.BINANCE_FUTURES))
        b.start_sync()
        self.c.gate.report_book(src, common, BookState.SYNCING)

    async def run_binance(self, src: Source, symbol: str, duration_s: float):
        futures = src == Source.BINANCE_FUTURES
        common = binance_to_common(symbol, futures)
        P = binance.FUTURES if futures else binance.SPOT
        s = symbol.lower()
        streams = [f"{s}@trade", f"{s}@bookTicker", f"{s}@depth@100ms"] + ([f"{s}@markPrice@1s"] if futures else [])
        url = WS_URLS[src] + "?streams=" + "/".join(streams)
        deadline = self.clock().timestamp() + duration_s
        delays, attempt = self.reconnect.delays(), 0
        while self.clock().timestamp() < deadline:
            conn = None
            try:
                conn = await self.ws.open(src, url)
                self.c.gate.on_connect(src, [common])
                self._begin_sync(src, common)
                await self._binance_snapshot(src, symbol, common)
                attempt = 0
                while self.clock().timestamp() < deadline:
                    raw = await asyncio.wait_for(conn.recv(), timeout=self.idle_timeout_s)
                    rx = self.clock()
                    try:
                        self._ingest_all(P.parse(json.loads(raw), rx))
                    except (EnvelopeError, KeyError, ValueError, TypeError) as e:
                        self.c.rejected.append({"error": f"{type(e).__name__}: {e}"})
                    b = self.c.books.get((src, common))
                    if b is not None and b.state == BookState.INVALID:
                        self.resyncs += 1
                        if self.resyncs > self.max_resyncs:
                            self.c.gate.on_resync_failed(src, common, b.reason or "")
                            raise ResyncExhausted(b.reason)
                        self._begin_sync(src, common)
                        await self._binance_snapshot(src, symbol, common)
            except (WsClosed, asyncio.TimeoutError, OSError) as e:
                self.c.gate.on_disconnect(src)
                self.reconnects += 1
                if attempt >= len(delays):
                    self.c.gate.on_resync_failed(src, common, f"reconnect attempts exhausted: {e}")
                    raise ResyncExhausted(str(e))
                await self.sleep(delays[attempt])
                attempt += 1
            finally:
                if conn is not None:
                    await conn.close()

    # ------------------------------------------------------------------ Toobit
    async def run_toobit(self, symbol: str, duration_s: float,
                         topics=("trade", "bookTicker", "markPrice", "depth")):
        src = Source.TOOBIT_FUTURES
        common = toobit_to_common(symbol)
        deadline = self.clock().timestamp() + duration_s
        delays, attempt = self.reconnect.delays(), 0
        while self.clock().timestamp() < deadline:
            conn = None
            try:
                conn = await self.ws.open(src, WS_URLS[src])
                self.c.gate.on_connect(src, [common])
                for t in topics:
                    await conn.send(json.dumps(toobit.ws_subscribe_message(t, symbol)))
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
                    rx = self.clock()
                    try:
                        self._ingest_all(toobit.parse_ws(json.loads(raw), rx))
                    except (EnvelopeError, KeyError, ValueError, TypeError) as e:
                        self.c.rejected.append({"error": f"{type(e).__name__}: {e}"})
            except (WsClosed, asyncio.TimeoutError, OSError) as e:
                self.c.gate.on_disconnect(src)
                self.reconnects += 1
                if attempt >= len(delays):
                    self.c.gate.on_resync_failed(src, common, f"reconnect attempts exhausted: {e}")
                    raise ResyncExhausted(str(e))
                await self.sleep(delays[attempt])
                attempt += 1
            finally:
                if conn is not None:
                    await conn.close()

    async def run(self, src: Source, symbol: str, duration_s: float):
        await self.sync_clock(src)
        await self.poll_reference(src, symbol)
        if src == Source.TOOBIT_FUTURES:
            await self.run_toobit(symbol, duration_s)
        else:
            await self.run_binance(src, symbol, duration_s)


# ---------------------------------------------------------------------- REST shape check (user-run)
async def check_rest(rest: PublicRestClient, src: Source, symbol: str) -> List[dict]:
    """Calls each documented public REST endpoint ONCE, runs the parser, and reports top-level keys
    and parse success. It records shapes, not data. Use it to compare reality with the docs."""
    rx = _utcnow()
    if src == Source.TOOBIT_FUTURES:
        T = [("/api/v1/time", None, None),
             ("/quote/v1/contract/ticker/price", {"symbol": symbol}, lambda r: toobit.parse_ticker_price(r, rx)),
             ("/quote/v1/trades", {"symbol": symbol, "limit": 5}, lambda r: toobit.parse_trades(symbol, r, rx)),
             ("/quote/v1/depth", {"symbol": symbol, "limit": 5}, lambda r: [toobit.parse_depth(symbol, r, rx)]),
             ("/quote/v1/contract/ticker/bookTicker", {"symbol": symbol}, lambda r: toobit.parse_book_ticker(r, rx)),
             ("/quote/v1/markPrice", {"symbol": symbol}, lambda r: toobit.parse_mark_price(r, rx)),
             ("/quote/v1/index", {"symbol": symbol}, lambda r: toobit.parse_index_price(r, rx)),
             ("/api/v1/futures/fundingRate", {"symbol": symbol}, lambda r: toobit.parse_funding_rate(r, rx)),
             ("/quote/v1/openInterest", {"symbol": symbol}, lambda r: toobit.parse_open_interest(r, rx)),
             ("/api/v1/exchangeInfo", None, lambda r: toobit.parse_contracts(r, rx))]
    else:
        fut = src == Source.BINANCE_FUTURES
        P = binance.FUTURES if fut else binance.SPOT
        pre = "/fapi/v1" if fut else "/api/v3"
        T = [(pre + "/time", None, None),
             (pre + "/ticker/price", {"symbol": symbol}, lambda r: [P.parse_price(r, rx)]),
             (pre + "/depth", {"symbol": symbol, "limit": 5}, lambda r: [P.parse_depth_snapshot(symbol, r, rx)])]
        if fut:
            T += [("/fapi/v1/openInterest", {"symbol": symbol}, lambda r: [P.parse_open_interest(r, rx)]),
                  ("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1}, lambda r: P.parse_funding_history(r, rx)),
                  ("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": "5m", "limit": 1},
                   lambda r: P.parse_long_short_ratio(r, rx))]
    out = []
    for path, params, fn in T:
        row = {"endpoint": path, "ok": False}
        try:
            raw = await asyncio.to_thread(rest.get, src, path, params)
            row["top_level"] = sorted(raw) if isinstance(raw, dict) else f"array[{len(raw)}]"
            if isinstance(raw, list) and raw and isinstance(raw[0], dict):
                row["first_item_keys"] = sorted(raw[0])
            n = len(fn(raw)) if fn else 0
            row.update(ok=True, parsed_events=n)
        except Exception as e:
            row["error"] = f"{type(e).__name__}: {e}"
        out.append(row)
    return out


def build_policy(args) -> NetworkPolicy:
    pol = NetworkPolicy(allow_binance=args.allow_binance_network, allow_toobit=args.allow_toobit_network,
                        toobit_user_approved=args.i_approve_toobit_public_readonly)
    return pol


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Public read-only market data runner (user-run).")
    ap.add_argument("--source", required=True, choices=[s.value for s in Source])
    ap.add_argument("--symbol", required=True, help="EXCHANGE symbol, e.g. BTCUSDT or BTC-SWAP-USDT")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--out", default=None, help="write envelopes as JSONL")
    ap.add_argument("--check", action="store_true", help="REST shape report vs parsers; no streaming")
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
        return 2
    rest = PublicRestClient(pol)
    if args.check:
        rep = asyncio.run(check_rest(rest, src, args.symbol))
        print(json.dumps(rep, indent=2))
        return 0 if all(r["ok"] for r in rep) else 1
    col = Collector()
    runner = LiveRunner(col, rest, PublicWsClient(pol))
    try:
        asyncio.run(runner.run(src, args.symbol, args.seconds))
    except ResyncExhausted as e:
        print(f"STOPPED: {e}", file=sys.stderr)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            for e in col.envelopes:
                fh.write(e.to_json() + "\n")
    print(json.dumps({"envelopes": len(col.envelopes), "rejected": len(col.rejected), "issues": col.gate.kinds(),
                      "reconnects": runner.reconnects, "resyncs": runner.resyncs}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
