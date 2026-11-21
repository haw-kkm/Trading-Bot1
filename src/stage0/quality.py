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
from collections import Counter, OrderedDict
from typing import Dict, List, Optional, Tuple

from .envelope import UNVERIFIED_FLAGS, EventType, MarketEnvelope, Source
from .orderbook import BookState
from .kline import KlineStore, MAX_CANDLES_PER_KEY, ANALYSIS_LOOKBACK


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


# Types that arrive as a continuous stream: their freshness is dropped on disconnect (data from a dead
# connection must not count) and they are the only ones subject to the receive-lag check.
STREAMING_TYPES = frozenset({"TRADE", "TICKER", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "ORDERBOOK_DELTA",
                             "MARK_PRICE", "INDEX_PRICE", "KLINE", "LIQUIDATION"})
LAG_CHECKED_TYPES = frozenset({"TRADE", "TICKER", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "ORDERBOOK_DELTA",
                               "MARK_PRICE", "KLINE"})
# Polled (slow) reference types may legitimately be older than a streaming tick.
SLOW_MAX_AGE = {"OPEN_INTEREST": timedelta(seconds=180), "LONG_SHORT_RATIO": timedelta(seconds=900),
                "FUNDING_RATE": timedelta(hours=2), "CONTRACT_SPEC": timedelta(days=2),
                "SERVER_TIME": timedelta(minutes=10)}


class _SeqWindow:
    """Bounded duplicate-detection memory: remembers the newest `cap` sequence ids (insertion order, FIFO eviction).
    A duplicate is detected exactly while its id is still inside the window; an id older than `cap` newer ids
    would be accepted again (documented trade-off for bounded memory)."""

    def __init__(self, cap: int):
        self.cap, self._d = cap, OrderedDict()

    def __contains__(self, k):
        return k in self._d

    def add(self, k):
        self._d[k] = None
        while len(self._d) > self.cap:
            self._d.popitem(last=False)

    def __len__(self):
        return len(self._d)


class DataQualityGate:
    """Freshness is tracked PER (source, symbol, event type). A consumer states which types it needs;
    READY requires every one of them to be present, fresh, timestamp-sane and verified."""

    def __init__(self, max_age: timedelta = timedelta(seconds=10),
                 max_receive_lag: timedelta = timedelta(seconds=5),
                 max_pair_skew: timedelta = timedelta(seconds=2),
                 divergence_pct: Decimal = Decimal("0.5"), max_seq_history: int = 100_000,
                 max_issues: int = 10_000, kline_lookback: timedelta = ANALYSIS_LOOKBACK,
                 max_candles_per_key: int = MAX_CANDLES_PER_KEY):
        self.max_age, self.max_receive_lag = max_age, max_receive_lag
        self.max_pair_skew, self.divergence_pct = max_pair_skew, divergence_pct
        self.issues: List[Issue] = []                       # newest `max_issues` only (bounded)
        self.issue_counts: Counter = Counter()              # exact totals by kind, never evicted
        self.max_issues, self.max_seq_history = max_issues, max_seq_history
        self._last: Dict[Tuple, MarketEnvelope] = {}        # (source,symbol,type) -> last accepted
        self._seen_seq: Dict[Tuple, _SeqWindow] = {}
        self._type_rx: Dict[Tuple, datetime] = {}           # (source,symbol,type) -> last GOOD received_at
        self._seen_types: Dict[Tuple, set] = {}             # (source,symbol) -> types ever seen
        self._bad_ts: Dict[Tuple, str] = {}                 # (source,symbol,type) -> reason (future / lag)
        self.max_age_by_type: Dict[str, timedelta] = dict(SLOW_MAX_AGE)
        self._conn: Dict[str, bool] = {}
        self._syncing: set = set()
        self._invalid: Dict[Tuple, str] = {}
        self._unverified: set = set()
        self._prices: Dict[Tuple, MarketEnvelope] = {}
        self._klines = KlineStore(cap=max_candles_per_key, lookback=kline_lookback)
        self._kline_rx: Dict[Tuple, datetime] = {}  # (source, common symbol, interval) -> latest valid received_at

    def _issue(self, kind, env_or_src, symbol="", detail=""):
        src = env_or_src.source.value if isinstance(env_or_src, MarketEnvelope) else str(env_or_src)
        self.issues.append(Issue(kind, src, symbol, detail))
        self.issue_counts[kind] += 1
        if len(self.issues) > self.max_issues:
            del self.issues[: len(self.issues) - self.max_issues]

    def is_current(self, env: MarketEnvelope) -> bool:
        """True only if `env` is the event the gate accepted as the latest state of its type (i.e. it passed
        every check: not BACKFILL, not duplicate, not out of order, sane timestamp, not crossed)."""
        if env.event_type == EventType.KLINE and env.source in (Source.BINANCE_SPOT, Source.BINANCE_FUTURES):
            from .kline import candle_key
            c = self._klines.rows.get(candle_key(env)) if env.normalized_payload.get("interval") else None
            return c is not None and (c.open_env is env or c.closed_env is env)
        return self._last.get((env.source.value, env.symbol, env.event_type.value)) is env

    def kinds(self):
        return [i.kind for i in self.issues]

    # ---- connection events -----------------------------------------------------------------
    def on_disconnect(self, source: Source):
        self._conn[source.value] = False
        self._issue("DISCONNECTED", source)
        # data received on the dead connection must not keep the symbol READY after a reconnect:
        for k in [k for k in self._type_rx if k[0] == source.value and k[2] in STREAMING_TYPES]:
            del self._type_rx[k]
        for k in [k for k in self._bad_ts if k[0] == source.value]:
            del self._bad_ts[k]
        for k in [k for k in self._kline_rx if k[0] == source.value]:
            del self._kline_rx[k]

    def on_connect(self, source: Source, symbols=()):
        """A (re)connection starts a re-sync: symbols stay SYNCING until mark_synced()."""
        self._conn[source.value] = True
        for s in symbols:
            self._syncing.add((source.value, s))

    def on_clock_offset(self, source: Source, offset_ms: int, max_ms: int = 1000):
        """Local-vs-exchange clock offset (from /time). Large skew makes lag/future checks unreliable."""
        self.clock_offsets = getattr(self, "clock_offsets", {})
        self.clock_offsets[source.value] = offset_ms
        if abs(offset_ms) > max_ms:
            self._issue("CLOCK_SKEW", source, "", f"{offset_ms} ms")

    def on_reference_failed(self, source: Source, symbol: str, event_type: str, detail: str = ""):
        """An optional reference datum could not be obtained. The type becomes a REQUIRED-but-missing type for
        any consumer of this symbol (status() -> STALE) until a real event of that type arrives."""
        self._seen_types.setdefault((source.value, symbol), set()).add(event_type)
        self._issue("REFERENCE_UNAVAILABLE", source, symbol, f"{event_type}: {detail}")

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
        self._seen_types.setdefault((src, sym), set()).add(env.event_type.value)
        if UNVERIFIED_FLAGS & set(env.quality_flags):      # schema OR semantic link (mapping / unit) unverified
            self._unverified.add((src, sym, env.event_type.value))
        key = (src, sym, env.event_type.value)

        def flag(kind, detail=""):
            found.append(kind)
            self._issue(kind, env, sym, detail)

        if "FUTURE_EVENT_TIME" in env.quality_flags:
            flag("FUTURE_EVENT_TIME", str(env.event_time))
        backfill = "BACKFILL" in env.quality_flags      # replayed history (e.g. Toobit first trade push)
        lag = (not backfill and env.event_type.value in LAG_CHECKED_TYPES and env.event_time is not None
               and env.received_at - env.event_time > self.max_receive_lag)
        if lag:
            flag("RECEIVE_LAG", str(env.received_at - env.event_time))
        if "FUTURE_EVENT_TIME" in env.quality_flags or lag:
            # an event with an insane timestamp is NOT evidence of fresh data: it does not refresh the
            # type, and the type stays INVALID until a sane event of the same type arrives.
            self._bad_ts[key] = "future event time" if "FUTURE_EVENT_TIME" in env.quality_flags else "receive lag"
            return found
        # A candle's opening time identifies a BAR, never its individual revisions. Keep
        # the existing generic sequence/order logic unchanged for every other event type.
        if env.event_type == EventType.KLINE and env.source in (Source.BINANCE_SPOT, Source.BINANCE_FUTURES):
            if backfill or UNVERIFIED_FLAGS & set(env.quality_flags):
                return found
            accepted, diagnostics = self._klines.observe(env)
            for kind, detail in diagnostics:
                flag(kind, detail)
            if not accepted:
                return found
            self._last[key] = env
            self._type_rx[key] = env.received_at
            self._kline_rx[(src, sym, env.normalized_payload["interval"])] = env.received_at
            self._bad_ts.pop(key, None)
            return found
        if env.sequence_id is not None and env.event_type != EventType.ORDERBOOK_SNAPSHOT:
            seen = self._seen_seq.setdefault(key, _SeqWindow(self.max_seq_history))
            if env.sequence_id in seen:
                flag("DUPLICATE_EVENT", env.sequence_id)
                return found                       # a duplicate must not update state
            seen.add(env.sequence_id)
        if backfill:
            return found          # recorded for de-duplication only: history is not evidence of live freshness
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
        self._type_rx[key] = env.received_at
        self._bad_ts.pop(key, None)
        if (src, sym) in self._invalid and self._invalid[(src, sym)] == "crossed book ticker":
            if env.event_type == EventType.BOOK_TICKER:
                self._invalid.pop((src, sym))     # a sane quote clears the quote-level fault
        kind = {EventType.TRADE: "last", EventType.TICKER: "last", EventType.MARK_PRICE: "mark",
                EventType.INDEX_PRICE: "index"}.get(env.event_type)
        if kind:
            self._prices[(src, sym, kind)] = env
        return found

    # ---- status ----------------------------------------------------------------------------
    def status(self, source: Source, symbol: str, now: datetime, types=None) -> Readiness:
        """`types`: event types the consumer relies on (EventType values / enums). None = every type
        seen for this symbol. Any of them carrying UNVERIFIED_SCHEMA => UNVERIFIED (never READY)."""
        src = source.value
        wanted = None if types is None else {getattr(t, "value", t) for t in types}
        if any(u[0] == src and u[1] == symbol and (wanted is None or u[2] in wanted)
               for u in self._unverified):
            return Readiness.UNVERIFIED
        if self._conn.get(src) is not True:
            return Readiness.DISCONNECTED
        if (src, symbol) in self._syncing:
            return Readiness.SYNCING
        if (src, symbol) in self._invalid:
            return Readiness.INVALID
        need = sorted(wanted) if wanted is not None else self._default_types(src, symbol)
        if not need:
            return Readiness.STALE                      # nothing ever received for this symbol
        for t in need:
            k = (src, symbol, t)
            if k in self._bad_ts:
                return Readiness.INVALID
            rx = self._type_rx.get(k)
            if rx is None:
                return Readiness.STALE                  # required type missing (never seen / dropped by disconnect)
            if now < rx or now - rx > self.max_age_by_type.get(t, self.max_age):
                return Readiness.STALE
            if t == "KLINE" and source in (Source.BINANCE_SPOT, Source.BINANCE_FUTURES):
                if not self.kline_ready_intervals(src, symbol, now):
                    return Readiness.STALE
        return Readiness.READY

    def kline_ready_intervals(self, source: str, symbol: str, now: datetime):
        """Intervals with an approved current CLOSED and fresh accepted WS update."""
        return self._klines.current_intervals(source, symbol, now, self._kline_rx, self.max_age)

    def approved_klines(self, source: str, symbol: str, now: datetime, lookback: timedelta):
        allowed = self.kline_ready_intervals(source, symbol, now)
        return [e for e in self._klines.approved(source, symbol, now, lookback)
                if e.normalized_payload["interval"] in allowed]

    def _default_types(self, src: str, symbol: str):
        """Types a consumer implicitly depends on when it names none: everything seen, except that a REST
        order-book snapshot which is followed by a delta stream is only BOOTSTRAP data (the delta stream and
        the book state govern freshness). Snapshot-only feeds (e.g. Toobit partial depth) keep their snapshot."""
        seen = set(self._seen_types.get((src, symbol), ()))
        if "ORDERBOOK_DELTA" in seen:
            seen.discard("ORDERBOOK_SNAPSHOT")
        return sorted(seen)

    def explain(self, source: Source, symbol: str, now: datetime, types=None) -> Dict[str, str]:
        """Per-type diagnosis (MISSING / STALE / BAD_TIMESTAMP / UNVERIFIED / OK) for logs and reports."""
        src = source.value
        need = sorted({getattr(t, "value", t) for t in types}) if types is not None else             self._default_types(src, symbol)
        out = {}
        for t in need:
            k = (src, symbol, t)
            rx = self._type_rx.get(k)
            if k in self._unverified:
                out[t] = "UNVERIFIED"
            elif k in self._bad_ts:
                out[t] = "BAD_TIMESTAMP: " + self._bad_ts[k]
            elif rx is None:
                out[t] = "MISSING"
            elif now - rx > self.max_age_by_type.get(t, self.max_age):
                out[t] = f"STALE ({(now - rx).total_seconds():.1f}s)"
            else:
                out[t] = "OK"
        return out

    def usable(self, source: Source, symbol: str, now: datetime, types=None) -> bool:
        """The ONLY question downstream may ask. False unless READY."""
        return self.status(source, symbol, now, types) == Readiness.READY

    # ---- cross-exchange divergence ---------------------------------------------------------
    def divergence(self, a: Source, b: Source, symbol: str, kind: str, now: datetime,
                   allow_basis: bool = False):
        """Returns Divergence or a reason string (never a number computed from unfit inputs).
        Same kind only (last/mark/index). Spot-vs-futures needs allow_basis=True (basis!=error).
        Both sources must be READY and their event times close (max_pair_skew)."""
        if (a == Source.BINANCE_SPOT) != (b == Source.BINANCE_SPOT) and not allow_basis:
            return "BASIS_NOT_COMPARABLE: spot vs futures requires explicit basis handling"
        ea, eb = self._prices.get((a.value, symbol, kind)), self._prices.get((b.value, symbol, kind))
        if ea is None or eb is None:
            return "NOT_COMPARABLE: missing price of this kind"
        for s_, e_ in ((a, ea), (b, eb)):
            st = self.status(s_, symbol, now, types=[e_.event_type.value])
            if st != Readiness.READY:
                return f"NOT_COMPARABLE: {s_.value} is {st.value}"
        for e_ in (ea, eb):                 # defence in depth: an unconfirmed mapping/unit is never compared
            bad = sorted(UNVERIFIED_FLAGS & set(e_.quality_flags))
            if bad:
                return f"NOT_COMPARABLE: {e_.source.value} {e_.event_type.value} carries {bad}"
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
