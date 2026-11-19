"""Data-quality / readiness gate (DATA-05).

Consumers (signal generation, entry decisions) must ask `gate.usable(source, symbol, now)`;
anything other than READY means "do not use". This module is NOT wired into the order engine.

Status precedence (first match wins):
  UNVERIFIED > DISCONNECTED > SYNCING > INVALID > STALE > READY
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple

from .envelope import EventType, MarketEnvelope, Source
from .orderbook import BookState


class Readiness(str, Enum):
    READY = "READY"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    SYNCING = "SYNCING"
    INVALID = "INVALID"
    UNVERIFIED = "UNVERIFIED"


@dataclass
class Issue:
    kind: str
    source: str
    symbol: str
    detail: str = ""


@dataclass
class Divergence:
    common_symbol: str
    kind: str
    price_a: Decimal
    price_b: Decimal
    pct: Decimal
    exceeds: bool


class DataQualityGate:
    def __init__(self, max_age: timedelta = timedelta(seconds=10),
                 max_receive_lag: timedelta = timedelta(seconds=5),
                 max_pair_skew: timedelta = timedelta(seconds=2),
                 divergence_pct: Decimal = Decimal("0.5")):
        self.max_age, self.max_receive_lag = max_age, max_receive_lag
        self.max_pair_skew, self.divergence_pct = max_pair_skew, divergence_pct
        self.issues: List[Issue] = []
        self._last: Dict[Tuple, MarketEnvelope] = {}        # (source,symbol,type) -> last accepted
        self._seen_seq: Dict[Tuple, set] = {}
        self._last_rx: Dict[Tuple, datetime] = {}           # (source,symbol) -> last received_at
        self._conn: Dict[str, bool] = {}
        self._syncing: set = set()
        self._invalid: Dict[Tuple, str] = {}
        self._unverified: set = set()
        self._prices: Dict[Tuple, MarketEnvelope] = {}

    def _issue(self, kind, env_or_src, symbol="", detail=""):
        src = env_or_src.source.value if isinstance(env_or_src, MarketEnvelope) else str(env_or_src)
        self.issues.append(Issue(kind, src, symbol, detail))

    def kinds(self):
        return [i.kind for i in self.issues]

    # ---- connection events -----------------------------------------------------------------
    def on_disconnect(self, source: Source):
        self._conn[source.value] = False
        self._issue("DISCONNECTED", source)

    def on_connect(self, source: Source, symbols=()):
        """A (re)connection starts a re-sync: symbols stay SYNCING until mark_synced()."""
        self._conn[source.value] = True
        for s in symbols:
            self._syncing.add((source.value, s))

    def mark_synced(self, source: Source, symbol: str):
        self._syncing.discard((source.value, symbol))
        self._invalid.pop((source.value, symbol), None)

    def on_resync_failed(self, source: Source, symbol: str, detail=""):
        self._syncing.discard((source.value, symbol))
        self._invalid[(source.value, symbol)] = f"resync failed: {detail}"
        self._issue("RESYNC_FAILED", source, symbol, detail)

    def report_book(self, source: Source, symbol: str, state: BookState, reason=None):
        key = (source.value, symbol)
        if state == BookState.VALID:
            self._invalid.pop(key, None)
            self._syncing.discard(key)
        elif state == BookState.SYNCING:
            self._syncing.add(key)
        elif state == BookState.INVALID:
            self._invalid[key] = reason or "book invalid"
            self._issue("BOOK_INVALID", source, symbol, reason or "")

    # ---- per-event checks ------------------------------------------------------------------
    def observe(self, env: MarketEnvelope) -> List[str]:
        """Record one event. Returns the issue kinds found (event may still be informational)."""
        found: List[str] = []
        src, sym = env.source.value, env.symbol
        if "UNVERIFIED_SCHEMA" in env.quality_flags:
            self._unverified.add(src)
        key = (src, sym, env.event_type.value)

        def flag(kind, detail=""):
            found.append(kind)
            self._issue(kind, env, sym, detail)

        if "FUTURE_EVENT_TIME" in env.quality_flags:
            flag("FUTURE_EVENT_TIME", str(env.event_time))
        if env.event_time and env.received_at - env.event_time > self.max_receive_lag:
            flag("RECEIVE_LAG", str(env.received_at - env.event_time))
        if env.sequence_id is not None:
            seen = self._seen_seq.setdefault(key, set())
            if env.sequence_id in seen:
                flag("DUPLICATE_EVENT", env.sequence_id)
                return found                       # a duplicate must not update state
            seen.add(env.sequence_id)
        prev = self._last.get(key)
        if prev is not None:
            if env.event_time and prev.event_time and env.event_time < prev.event_time:
                flag("OUT_OF_ORDER", f"{env.event_time} < {prev.event_time}")
                return found                       # stale event must not overwrite newer state
            if (env.event_type == EventType.ORDERBOOK_DELTA and env.sequence_id is not None
                    and prev.sequence_id is not None and int(env.sequence_id) < int(prev.sequence_id)):
                flag("OUT_OF_ORDER", "sequence regressed")
                return found
        n = env.normalized_payload
        if env.event_type == EventType.BOOK_TICKER and n["bid_price"] >= n["ask_price"]:
            flag("CROSSED_BOOK", f"{n['bid_price']} >= {n['ask_price']}")
            self._invalid[(src, sym)] = "crossed book ticker"
            return found
        if env.event_type in (EventType.ORDERBOOK_SNAPSHOT, EventType.ORDERBOOK_DELTA) and n["bids"] and n["asks"]:
            if env.event_type == EventType.ORDERBOOK_SNAPSHOT and \
                    max(p for p, _ in n["bids"]) >= min(p for p, _ in n["asks"]):
                flag("CROSSED_BOOK", "snapshot crossed")
                self._invalid[(src, sym)] = "crossed snapshot"
                return found
        self._last[key] = env
        self._last_rx[(src, sym)] = env.received_at
        if (src, sym) in self._invalid and self._invalid[(src, sym)] == "crossed book ticker":
            if env.event_type == EventType.BOOK_TICKER:
                self._invalid.pop((src, sym))     # a sane quote clears the quote-level fault
        kind = {EventType.TRADE: "last", EventType.TICKER: "last", EventType.MARK_PRICE: "mark",
                EventType.INDEX_PRICE: "index"}.get(env.event_type)
        if kind:
            self._prices[(src, sym, kind)] = env
        return found

    # ---- status ----------------------------------------------------------------------------
    def status(self, source: Source, symbol: str, now: datetime) -> Readiness:
        src = source.value
        if src in self._unverified:
            return Readiness.UNVERIFIED
        if self._conn.get(src) is not True:
            return Readiness.DISCONNECTED
        if (src, symbol) in self._syncing:
            return Readiness.SYNCING
        if (src, symbol) in self._invalid:
            return Readiness.INVALID
        last = self._last_rx.get((src, symbol))
        if last is None or now - last > self.max_age:
            return Readiness.STALE
        return Readiness.READY

    def usable(self, source: Source, symbol: str, now: datetime) -> bool:
        """The ONLY question downstream may ask. False unless READY."""
        return self.status(source, symbol, now) == Readiness.READY

    # ---- cross-exchange divergence ---------------------------------------------------------
    def divergence(self, a: Source, b: Source, symbol: str, kind: str, now: datetime,
                   allow_basis: bool = False):
        """Returns Divergence or a reason string (never a number computed from unfit inputs).
        Same kind only (last/mark/index). Spot-vs-futures needs allow_basis=True (basis!=error).
        Both sources must be READY and their event times close (max_pair_skew)."""
        if (a == Source.BINANCE_SPOT) != (b == Source.BINANCE_SPOT) and not allow_basis:
            return "BASIS_NOT_COMPARABLE: spot vs futures requires explicit basis handling"
        for s in (a, b):
            st = self.status(s, symbol, now)
            if st != Readiness.READY:
                return f"NOT_COMPARABLE: {s.value} is {st.value}"
        ea, eb = self._prices.get((a.value, symbol, kind)), self._prices.get((b.value, symbol, kind))
        if ea is None or eb is None:
            return "NOT_COMPARABLE: missing price of this kind"
        ta, tb = ea.event_time or ea.received_at, eb.event_time or eb.received_at
        if abs(ta - tb) > self.max_pair_skew:
            return f"NOT_COMPARABLE: timestamps differ by {abs(ta - tb)}"
        field = {"last": "price", "mark": "mark_price", "index": "index_price"}[kind]
        pa, pb = ea.normalized_payload[field], eb.normalized_payload[field]
        pct = abs(pa - pb) / ((pa + pb) / 2) * 100
        d = Divergence(symbol, kind, pa, pb, pct, pct > self.divergence_pct)
        if d.exceeds:
            self._issue("PRICE_DIVERGENCE", f"{a.value}/{b.value}", symbol, f"{kind} {pct:.4f}%")
        return d
