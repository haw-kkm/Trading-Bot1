"""Collection runner: FIXTURE replay (default, zero network) and PUBLIC_READ_ONLY plumbing.

SAFETY
  * NetworkPolicy denies everything by default; Binance and Toobit are switched separately.
    Toobit has no way to be enabled from config here without an explicit code-level opt-in
    (TOOBIT_NETWORK_APPROVED) that is False and must stay so until the user approves.
  * Only HTTP GET of an allow-listed set of PUBLIC paths. No signing, no API keys, no order
    endpoints: secrets passed in are never read (the client has no parameter for them).
  * The default transport is only reachable after policy.check() passes. Tests inject fakes.
"""
import json
import pathlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional

from .adapters import binance, toobit
from .envelope import EnvelopeError, EventType, MarketEnvelope, Source, utc
from .orderbook import BookState, LocalOrderBook
from .quality import DataQualityGate

TOOBIT_NETWORK_APPROVED = False     # user has NOT approved real Toobit access. Do not flip casually.


class Mode(str, Enum):
    FIXTURE = "FIXTURE"
    PUBLIC_READ_ONLY = "PUBLIC_READ_ONLY"


class NetworkDisabled(RuntimeError):
    pass


@dataclass
class NetworkPolicy:
    allow_binance: bool = False
    allow_toobit: bool = False
    # Set ONLY from the explicit run-time flag --i-approve-toobit-public-readonly typed by the user
    # (see live.py). Code/config/tests must never set it; TOOBIT_NETWORK_APPROVED stays False.
    toobit_user_approved: bool = False

    def check(self, source: Source) -> None:
        if source == Source.TOOBIT_FUTURES:
            if not (self.allow_toobit and (TOOBIT_NETWORK_APPROVED or self.toobit_user_approved)):
                raise NetworkDisabled("Toobit network access is disabled (needs the user's run-time approval)")
        elif not self.allow_binance:
            raise NetworkDisabled("Binance network access is disabled (no explicit allow flag)")


# public, unauthenticated, read-only paths only
PUBLIC_PATHS = {
    Source.BINANCE_SPOT: {"/api/v3/time", "/api/v3/ticker/price", "/api/v3/depth", "/api/v3/klines"},
    Source.BINANCE_FUTURES: {"/fapi/v1/time", "/fapi/v1/ticker/price", "/fapi/v1/depth", "/fapi/v1/klines",
                             "/fapi/v1/premiumIndex", "/fapi/v1/openInterest", "/fapi/v1/fundingRate",
                             "/futures/data/globalLongShortAccountRatio", "/fapi/v1/exchangeInfo"},
    # Toobit public market-data paths from https://api-docs.toobit.com (read 2026-11-20). GET only.
    Source.TOOBIT_FUTURES: {"/api/v1/time", "/api/v1/exchangeInfo", "/quote/v1/depth", "/quote/v1/trades",
                            "/quote/v1/contract/ticker/price", "/quote/v1/contract/ticker/bookTicker",
                            "/quote/v1/markPrice", "/quote/v1/index", "/api/v1/futures/fundingRate",
                            "/quote/v1/openInterest", "/quote/v1/klines",
                            "/quote/v1/indexPriceComponents"},      # extended-semantics evidence only (docs 2026-11-20)
}
# Toobit REST host: "The base endpoint is: https://api.toobit.com"
# (https://api-docs.toobit.com/api/basic-information.html, read 2026-11-20). Live reachability: unverified.
HOSTS = {Source.BINANCE_SPOT: "https://api.binance.com", Source.BINANCE_FUTURES: "https://fapi.binance.com",
         Source.TOOBIT_FUTURES: "https://api.toobit.com"}
# Binance USDS-M futures moved to routed paths (legacy /stream and /ws were announced for removal on
# 2026-04-23): /public = bookTicker + depth, /market = aggTrade, markPrice, kline, forceOrder.
# Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
BINANCE_FUTURES_WS = {"public": "wss://fstream.binance.com/public/stream",
                      "market": "wss://fstream.binance.com/market/stream"}
WS_URLS = {Source.BINANCE_SPOT: "wss://stream.binance.com:9443/stream",
           Source.BINANCE_FUTURES: BINANCE_FUTURES_WS["public"],
           Source.TOOBIT_FUTURES: "wss://stream.toobit.com/quote/ws/v1"}
WS_ALLOWED = {Source.BINANCE_SPOT: (WS_URLS[Source.BINANCE_SPOT],),
              Source.BINANCE_FUTURES: tuple(BINANCE_FUTURES_WS.values()),
              Source.TOOBIT_FUTURES: (WS_URLS[Source.TOOBIT_FUTURES],)}
_FORBIDDEN_PARAMS = {"signature", "apikey", "api_key", "timestamp_sig", "recvwindow"}


class PublicRestClient:
    def __init__(self, policy: NetworkPolicy, transport: Optional[Callable[[str, Dict], object]] = None):
        self.policy = policy
        self._transport = transport

    def get(self, source: Source, path: str, params: Optional[Dict] = None):
        self.policy.check(source)                                  # 1) policy first
        if path not in PUBLIC_PATHS.get(source, set()):
            raise NetworkDisabled(f"path not in public allow-list: {path}")
        params = dict(params or {})
        if any(k.lower() in _FORBIDDEN_PARAMS for k in params):
            raise NetworkDisabled("authentication/signing parameters are not allowed")
        url = HOSTS[source] + path
        transport = self._transport or _urllib_get
        return transport(url, params)


class HttpError(Exception):
    """HTTP status != 2xx. Carries status and LOWER-CASED response headers only (never the body)."""

    def __init__(self, status: int, headers: Optional[Dict[str, str]] = None):
        super().__init__(f"HTTP {status}")
        self.status, self.headers = status, {k.lower(): v for k, v in (headers or {}).items()}


class TransportError(Exception):
    """No HTTP response: timeout / connection failure. kind in {"timeout", "connection"}."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


class ResponseFormatError(Exception):
    """2xx but not valid JSON (permanent for this request)."""


def _urllib_get(url: str, params: Dict):            # pragma: no cover - never reached in tests
    """Plain unauthenticated GET. No API-key header, no signature, whatever the environment holds."""
    import socket
    import urllib.error
    import urllib.parse
    import urllib.request
    q = urllib.parse.urlencode(params)
    req = urllib.request.Request(url + ("?" + q if q else ""), method="GET",
                                 headers={"User-Agent": "stage0-public-readonly", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:   # noqa: S310
            body = r.read().decode()
    except urllib.error.HTTPError as e:
        raise HttpError(e.code, dict(e.headers or {}))
    except (socket.timeout, TimeoutError):
        raise TransportError("timeout")
    except urllib.error.URLError as e:
        raise TransportError("timeout" if isinstance(e.reason, (socket.timeout, TimeoutError)) else "connection")
    except OSError:
        raise TransportError("connection")
    try:
        return json.loads(body)
    except ValueError:
        raise ResponseFormatError("invalid JSON body")


@dataclass
class ReconnectPolicy:
    """Exponential backoff schedule (computed, never slept here)."""
    base_s: float = 1.0
    cap_s: float = 60.0
    max_attempts: int = 8

    def delays(self) -> List[float]:
        return [min(self.cap_s, self.base_s * (2 ** i)) for i in range(self.max_attempts)]


class SubscriptionManager:
    """Remembers desired streams so they can be re-subscribed after a reconnect."""

    def __init__(self):
        self.streams: Dict[Source, set] = {}

    def add(self, source: Source, stream: str):
        self.streams.setdefault(source, set()).add(stream)

    def resubscribe_plan(self, source: Source) -> List[str]:
        return sorted(self.streams.get(source, ()))

    @staticmethod
    def binance_streams(symbol: str, kinds=("trade", "bookTicker", "depth@100ms")) -> List[str]:
        return [f"{symbol.lower()}@{k}" for k in kinds]


@dataclass
class Collector:
    gate: DataQualityGate = field(default_factory=DataQualityGate)
    envelopes: List[MarketEnvelope] = field(default_factory=list)
    rejected: List[dict] = field(default_factory=list)
    # Long-run memory bounds. None = keep everything (fixture replay, tests). For a live run set max_envelopes
    # (newest N kept in memory) and stream every envelope to `sink` BEFORE it can be evicted, so no evidence is
    # lost. Totals below are exact regardless of eviction.
    max_envelopes: Optional[int] = None
    max_rejected: Optional[int] = None
    sink: Optional[Callable[[MarketEnvelope], None]] = None
    rejected_sink: Optional[Callable[[dict], None]] = None  # durable raw rejection evidence
    envelope_count: int = 0
    rejected_count: int = 0
    books: Dict[tuple, LocalOrderBook] = field(default_factory=dict)
    network_calls: int = 0           # real network requests made by THIS object (always 0 in FIXTURE)

    def reject(self, entry: dict):
        """Quarantine a rejected message; persist evidence before bounded-memory eviction."""
        if self.rejected_sink is not None and "raw_message" in entry:
            self.rejected_sink(entry)
        self.rejected_count += 1
        self.rejected.append(entry)
        if self.max_rejected is not None and len(self.rejected) > self.max_rejected:
            del self.rejected[: len(self.rejected) - self.max_rejected]

    @property
    def total_envelopes(self) -> int:
        return max(self.envelope_count, len(self.envelopes))

    @property
    def total_rejected(self) -> int:
        return max(self.rejected_count, len(self.rejected))

    def _book(self, env: MarketEnvelope) -> LocalOrderBook:
        k = (env.source, env.symbol)
        if k not in self.books:
            self.books[k] = LocalOrderBook(futures=env.source == Source.BINANCE_FUTURES)
        return self.books[k]

    def _ingest(self, env: MarketEnvelope):
        self.envelope_count += 1
        if self.sink is not None:
            self.sink(env)                      # durable record first; eviction below can then never lose it
        self.envelopes.append(env)
        if self.max_envelopes is not None and len(self.envelopes) > self.max_envelopes:
            del self.envelopes[: len(self.envelopes) - self.max_envelopes]
        found = self.gate.observe(env)
        if "DUPLICATE_EVENT" in found or "OUT_OF_ORDER" in found:
            return
        if env.source in (Source.BINANCE_SPOT, Source.BINANCE_FUTURES):
            n = env.normalized_payload
            if env.event_type == EventType.ORDERBOOK_DELTA:
                b = self._book(env)
                b.on_delta(n["first_update_id"], n["final_update_id"], n["bids"], n["asks"],
                           n.get("prev_final_update_id"))
                self.gate.report_book(env.source, env.symbol, b.state, b.reason)
            elif env.event_type == EventType.ORDERBOOK_SNAPSHOT:
                b = self._book(env)
                if b.state != BookState.SYNCING:
                    b.start_sync()
                b.apply_snapshot(n["last_update_id"], n["bids"], n["asks"])
                self.gate.report_book(env.source, env.symbol, b.state, b.reason)
                if b.state == BookState.VALID:
                    self.gate.mark_synced(env.source, env.symbol)

    def handle(self, rec: dict):
        rx = utc(rec["t"], "t")
        src = Source(rec["source"])
        kind = rec["kind"]
        msg = rec.get("msg")
        if kind == "control":
            from .symbols import binance_to_common, toobit_to_common
            xsym = rec.get("symbol", "")       # control records carry the EXCHANGE symbol
            if not xsym:
                common = ""
            elif src == Source.TOOBIT_FUTURES:
                common = toobit_to_common(xsym)
            else:
                common = binance_to_common(xsym, src == Source.BINANCE_FUTURES)
            act = rec["action"]
            if act == "connect":
                self.gate.on_connect(src, [common] if common else [])
            elif act == "disconnect":
                self.gate.on_disconnect(src)
            elif act == "start_sync":
                b = self.books.setdefault((src, common), LocalOrderBook(src == Source.BINANCE_FUTURES))
                b.start_sync()
                self.gate.report_book(src, common, BookState.SYNCING)
            elif act == "synced":
                self.gate.mark_synced(src, common)
            elif act == "resync_failed":
                self.gate.on_resync_failed(src, common, rec.get("detail", ""))
            else:
                raise EnvelopeError(f"unknown control action {act!r}")
            return
        if src == Source.TOOBIT_FUTURES:
            p = {"ticker": lambda: toobit.parse_ticker_price(msg, rx),
                 "trades": lambda: toobit.parse_trades(rec["symbol"], msg, rx),
                 "depth": lambda: [toobit.parse_depth(rec["symbol"], msg, rx)],
                 "book_ticker": lambda: toobit.parse_book_ticker(msg, rx),
                 "mark_price": lambda: toobit.parse_mark_price(msg, rx),
                 "index_price": lambda: toobit.parse_index_price(msg, rx),
                 "funding_rate": lambda: toobit.parse_funding_rate(msg, rx),
                 "open_interest": lambda: toobit.parse_open_interest(msg, rx),
                 "contract": lambda: toobit.parse_contracts(msg, rx),
                 "ws": lambda: toobit.parse_ws(msg, rx)}
        else:
            P = binance.FUTURES if src == Source.BINANCE_FUTURES else binance.SPOT
            p = {"ws": lambda: P.parse(msg, rx),
                 "depth_snapshot": lambda: [P.parse_depth_snapshot(rec["symbol"], msg, rx)],
                 "price": lambda: [P.parse_price(msg, rx)],
                 "open_interest": lambda: [P.parse_open_interest(msg, rx)],
                 "funding_history": lambda: P.parse_funding_history(msg, rx),
                 "long_short_ratio": lambda: P.parse_long_short_ratio(msg, rx),
                 "exchange_info_symbol": lambda: [P.parse_exchange_info_symbol(msg, rx)]}
        if kind not in p:
            raise EnvelopeError(f"unknown record kind {kind!r} for {src.value}")
        for env in p[kind]():
            self._ingest(env)

    def run_fixture(self, path) -> "Collector":
        """Replay a JSONL file. Bad records are quarantined in `rejected`, never silently used."""
        for i, line in enumerate(pathlib.Path(path).read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                self.handle(json.loads(line))
            except (EnvelopeError, KeyError, ValueError) as e:
                self.reject({"line": i, "error": f"{type(e).__name__}: {e}"})
        return self


def run(mode: Mode = Mode.FIXTURE, fixture: Optional[str] = None, policy: Optional[NetworkPolicy] = None):
    if mode == Mode.FIXTURE:
        if not fixture:
            raise ValueError("FIXTURE mode needs a fixture path")
        return Collector().run_fixture(fixture)
    policy = policy or NetworkPolicy()
    # PUBLIC_READ_ONLY: refuse unless at least one explicit allow exists. The streaming loop is
    # intentionally NOT implemented here (needs a WebSocket transport + user approval).
    if not (policy.allow_binance or policy.allow_toobit):
        raise NetworkDisabled("PUBLIC_READ_ONLY requires an explicit allow flag; none given")
    raise NotImplementedError("live streaming loop not implemented; REST/WS clients exist but are unverified live")


if __name__ == "__main__":
    import sys
    c = run(Mode.FIXTURE, sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures/stage0/session_basic.jsonl")
    print(f"envelopes={len(c.envelopes)} rejected={len(c.rejected)} issues={c.gate.kinds()}")
