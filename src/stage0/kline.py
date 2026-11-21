"""Bounded Binance 1m/5m candle state machine. No network or trading operations.

Event time (E) is not a sequence number. A candle is keyed by source, common and
exchange symbols, interval and opening time. Arrival timestamps never choose a winner.
The 1s stream and timezone-offset variants are deliberately NOT_SUPPORTED.
"""
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional, Tuple

from .envelope import EnvelopeError, EventType, MarketEnvelope, Source
from .symbols import binance_to_common

INTERVAL_MS = {"1m": 60_000, "5m": 300_000}
MAX_CANDLES_PER_KEY = 256
ANALYSIS_LOOKBACK = timedelta(hours=1)
GRACE_MS = 5000
PREMATURE_TOLERANCE_MS = 1000
FINALIZATION_WARN_MS = 5000


def millis(dt: datetime) -> int:
    """Exact integer milliseconds for UTC-aware timestamps."""
    return (dt - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)


def candle_key(env: MarketEnvelope):
    n = env.normalized_payload
    return (env.source.value, env.symbol, env.exchange_symbol, n["interval"], n["open_time_ms"])


def fingerprint(env: MarketEnvelope) -> str:
    """Original exchange message plus canonical normalized values, excluding LOCAL received_at/digest."""
    return json.dumps((env.source.value, env.exchange_symbol, env.event_time.isoformat()
                       if env.event_time else None, env.raw_payload, env.normalized_payload),
                      sort_keys=True, separators=(",", ":"), default=str)


def close_values(env: MarketEnvelope):
    n = env.normalized_payload
    return tuple(n[k] for k in ("open", "high", "low", "close", "volume", "interval",
                                "open_time_ms", "close_time_ms", "is_closed"))


def validate(env: MarketEnvelope, premature_ms: int = PREMATURE_TOLERANCE_MS,
             late_warn_ms: int = FINALIZATION_WARN_MS):
    """Return diagnostics after strict defensive validation; raise for invalid Binance candles.

    A raw Binance kline, if present, must agree with the normalized payload. Synthetic/manual
    envelopes are not trusted either: they must supply all normalized fields and pass identical
    candle, OHLC, clock and interval constraints.
    """
    if env.event_type != EventType.KLINE or env.source not in (Source.BINANCE_SPOT, Source.BINANCE_FUTURES):
        raise EnvelopeError("only Binance KLINE is supported by this state machine")
    n = env.normalized_payload
    iv = n.get("interval")
    if iv not in INTERVAL_MS:
        raise EnvelopeError(f"KLINE_NOT_SUPPORTED interval {iv!r}")
    span = INTERVAL_MS[iv]
    start, finish, closed = n.get("open_time_ms"), n.get("close_time_ms"), n.get("is_closed")
    if type(start) is not int or type(finish) is not int:
        raise EnvelopeError("KLINE timestamps must be integer milliseconds")
    if start < 0 or start % span or finish != start + span - 1:
        raise EnvelopeError("KLINE interval alignment or close_time_ms invalid")
    if type(closed) is not bool:
        raise EnvelopeError("KLINE is_closed must be a JSON boolean")
    if not env.exchange_symbol or binance_to_common(
            env.exchange_symbol, env.source == Source.BINANCE_FUTURES) != env.symbol:
        raise EnvelopeError("KLINE source/symbol mapping mismatch")
    if env.event_time is None:
        raise EnvelopeError("KLINE event_time E is required")
    end = start + span
    event_ms = millis(env.event_time)
    if event_ms < start:
        raise EnvelopeError("KLINE event_time before candle opens")
    if not (n["low"] <= n["open"] <= n["high"] and n["low"] <= n["close"] <= n["high"]):
        raise EnvelopeError("KLINE_OHLC_INVALID")
    for opt in ("trade_count", "first_trade_id", "last_trade_id"):
        if opt in n and (type(n[opt]) is not int or n[opt] < 0):
            raise EnvelopeError(f"KLINE {opt} must be a nonnegative integer")
    raw = env.raw_payload
    if "k" in raw:
        k = raw["k"]
        if not isinstance(k, dict) or raw.get("e") != "kline":
            raise EnvelopeError("KLINE raw payload malformed")
        if raw.get("s") != env.exchange_symbol or k.get("s") != env.exchange_symbol:
            raise EnvelopeError("KLINE raw symbol mismatch")
        if type(raw.get("E")) is not int or raw["E"] != event_ms:
            raise EnvelopeError("KLINE raw E mismatch")
        if any(k.get(field) != val for field, val in
               (("t", start), ("T", finish), ("i", iv), ("x", closed))):
            raise EnvelopeError("KLINE raw candle metadata mismatch")
        for field, name in (("o", "open"), ("h", "high"), ("l", "low"),
                            ("c", "close"), ("v", "volume")):
            try:
                if Decimal(str(k[field])) != n[name]:
                    raise EnvelopeError(f"KLINE raw {field} mismatch")
            except (KeyError, ValueError, ArithmeticError):
                raise EnvelopeError(f"KLINE raw {field} malformed")
    if closed and event_ms < end - premature_ms:
        raise EnvelopeError("KLINE_PREMATURE_CLOSE")
    notices = []
    if closed and event_ms < end:
        notices.append(("CLOSE_EARLY_BY_MS", str(end - event_ms)))
    if closed and event_ms - end > late_warn_ms:
        notices.append(("KLINE_LATE_FINALIZATION", str(event_ms - end)))
    return notices


def monotone(prev: MarketEnvelope, nxt: MarketEnvelope) -> bool:
    a, b = prev.normalized_payload, nxt.normalized_payload
    if b["high"] < a["high"] or b["low"] > a["low"] or b["volume"] < a["volume"]:
        return False
    for field in ("trade_count", "last_trade_id"):
        if field in a and field in b and b[field] < a[field]:
            return False
    return True


@dataclass
class Candle:
    open_env: Optional[MarketEnvelope] = None
    previous_open: Optional[MarketEnvelope] = None  # rollback point BEFORE the disputed E
    closed_env: Optional[MarketEnvelope] = None
    ambiguous_e: Optional[int] = None
    state: str = "OPEN"       # OPEN, CLOSED, SUSPECT, CONFLICT
    last_observed: Optional[datetime] = None


class KlineStore:
    """Per-bar admission with bounded history, reversible OPEN ambiguity and sticky tombstones."""

    def __init__(self, cap=MAX_CANDLES_PER_KEY, lookback=ANALYSIS_LOOKBACK,
                 grace_ms=GRACE_MS, premature_ms=PREMATURE_TOLERANCE_MS,
                 late_warn_ms=FINALIZATION_WARN_MS):
        if cap < 1 or lookback <= timedelta(0) or grace_ms < 0:
            raise ValueError("invalid KlineStore limits")
        self.cap, self.lookback = cap, lookback
        self.grace_ms, self.premature_ms, self.late_warn_ms = grace_ms, premature_ms, late_warn_ms
        self.rows: Dict[Tuple, Candle] = {}

    def _prune(self, env: MarketEnvelope):
        # Keep enough time for lookback and delayed next candle, but cap independently per interval.
        n = env.normalized_payload
        prefix = candle_key(env)[:-1]
        span = INTERVAL_MS[n["interval"]]
        cutoff = millis(env.received_at) - int(self.lookback.total_seconds() * 1000) - 2 * span \
                 - self.grace_ms - 5000
        keys = sorted((k for k in self.rows if k[:-1] == prefix), key=lambda k: k[-1])
        for k in keys:
            if k[-1] < cutoff:
                del self.rows[k]
        keys = sorted((k for k in self.rows if k[:-1] == prefix), key=lambda k: k[-1])
        for k in keys[:-self.cap]:
            del self.rows[k]

    def observe(self, env: MarketEnvelope):
        """(accepted, [(diagnostic, value), ...]); rejected updates never refresh stream freshness."""
        try:
            notices = validate(env, self.premature_ms, self.late_warn_ms)
        except EnvelopeError as exc:
            msg = str(exc)
            return False, [(msg.split()[0] if msg.startswith("KLINE_") else "KLINE_INVALID", msg)]
        key = candle_key(env)
        c = self.rows.get(key)
        if c is None:
            c = Candle()
            self.rows[key] = c
        closed = env.normalized_payload["is_closed"]
        stamp = millis(env.event_time)
        if c.state == "CONFLICT":
            return False, [("KLINE_CONFLICT", "tombstone remains")]
        if c.state == "SUSPECT":
            return False, [("KLINE_SUSPECT", "nonmonotone tombstone remains")]
        if c.state == "CLOSED":
            if not closed:
                return False, [("LATE_FORMING_UPDATE", "final candle is immutable")]
            if close_values(c.closed_env) == close_values(env):
                return False, [("DUPLICATE_EVENT", "identical finalized values")]
            c.state = "CONFLICT"
            c.closed_env = None
            return False, [("KLINE_CONFLICT", "conflicting finalized OHLCV")]
        if closed:
            trusted = c.open_env
            if trusted is not None and not monotone(trusted, env):
                c.state = "SUSPECT"
                c.open_env = None
                return False, [("KLINE_NONMONOTONE_COUNT", "CLOSED regressed from trusted OPEN")]
            c.closed_env, c.open_env, c.state = env, None, "CLOSED"
            c.ambiguous_e = None
            c.last_observed = env.received_at
            self._prune(env)
            return True, notices
        if c.ambiguous_e is not None and stamp <= c.ambiguous_e:
            return False, [("KLINE_E_TIE_AMBIGUOUS", "disputed E or older revision excluded")]
        if c.open_env is None:
            c.ambiguous_e = None
            c.open_env, c.last_observed = env, env.received_at
            self._prune(env)
            return True, notices
        prior_e = millis(c.open_env.event_time)
        if stamp < prior_e:
            return False, [("OUT_OF_ORDER", "older OPEN revision")]
        if stamp == prior_e:
            if fingerprint(c.open_env) == fingerprint(env):
                return False, [("DUPLICATE_EVENT", "identical raw KLINE")]
            # Undo FIRST tied candidate; neither payload remains trusted. The previous revision
            # was recorded at the prior strictly increasing E. Receipt freshness cannot be undone.
            c.open_env, c.ambiguous_e = c.previous_open, stamp
            return False, [("KLINE_E_TIE_AMBIGUOUS", "both tied OPEN payloads excluded")]
        if not monotone(c.open_env, env):
            c.state = "SUSPECT"
            c.open_env = None
            return False, [("KLINE_NONMONOTONE_COUNT", "OPEN revision regressed")]
        c.previous_open, c.open_env = c.open_env, env
        c.ambiguous_e = None
        c.last_observed = env.received_at
        self._prune(env)
        return True, notices

    def approved(self, source: str, symbol: str, now: datetime, lookback: timedelta):
        start = now - lookback
        out = []
        for key, candle in self.rows.items():
            if key[0] != source or key[1] != symbol or candle.state != "CLOSED":
                continue
            env = candle.closed_env
            if env and env.received_at <= now and start <= env.event_time <= now:
                out.append(env)
        return sorted(out, key=lambda e: (e.normalized_payload["interval"],
                                          e.normalized_payload["open_time_ms"]))

    def current_intervals(self, source: str, symbol: str, now: datetime,
                          rx_by_interval: dict, max_age: timedelta):
        """Currency C applies to the newest APPROVED CLOSED, never every historical candle.

        A newer ambiguous/suspect bar cannot leave stale earlier candles silently READY.
        The freshness check is separate for 1m and 5m streams.
        """
        now_ms = millis(now)
        ok = set()
        intervals = {(k[2], k[3]) for k in self.rows if k[0] == source and k[1] == symbol}
        for xs, iv in intervals:
            rx = rx_by_interval.get((source, symbol, iv))
            if rx is None or now < rx or now - rx > max_age:
                continue
            rows = [(k[-1], c) for k, c in self.rows.items()
                    if k[0] == source and k[1] == symbol and k[2] == xs and k[3] == iv]
            if not rows:
                continue
            _, newest = max(rows, key=lambda t: t[0])
            if newest.state in ("SUSPECT", "CONFLICT") or newest.ambiguous_e is not None:
                continue
            closed = [(op, c) for op, c in rows if c.state == "CLOSED"]
            if not closed:
                continue
            start, _ = max(closed, key=lambda t: t[0])
            span = INTERVAL_MS[iv]
            if start + span <= now_ms <= start + 2 * span + self.grace_ms:
                ok.add(iv)
        return ok

    def current_closed(self, source: str, symbol: str, now: datetime) -> bool:
        rx_by_interval = {(k[0], k[1], k[3]): now for k in self.rows}
        return bool(self.current_intervals(source, symbol, now, rx_by_interval, timedelta(seconds=10)))

    @staticmethod
    def supported_lookback(lookback: timedelta, cap: int = MAX_CANDLES_PER_KEY) -> bool:
        # Reserve two intervals for overlap and delayed closure, plus one margin candle.
        return lookback > timedelta(0) and \
               int((lookback.total_seconds() + 59) // 60) + 3 <= cap
