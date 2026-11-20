"""Stage 0 market-data envelope (DATA-04).

Reliability score (documented, deterministic, always in [0, 1]):
    base      = 1.0 for OBSERVED, 0.8 for DERIVED, 0.5 for ESTIMATED
    penalty   = 0.25 per quality flag in PENALTY_FLAGS (other flags are informational)
    score     = clamp(base - penalty, 0, 1)
It measures PROVENANCE AND KNOWN DEFECTS only, not exchange trustworthiness.

event_time  = time the exchange says the event happened (UTC; None if the feed gives none).
received_at = time WE received it (UTC, set by the collector/clock, never by the feed).

Integrity: `digest` is SHA-256 over the canonical content; from_dict() recomputes it, so any
edit of a serialised envelope is rejected (tamper detection, not authentication).
"""
import copy
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, Optional

SCHEMA_VERSION = "1.0"
FUTURE_SKEW = timedelta(seconds=5)


class Source(str, Enum):
    BINANCE_SPOT = "binance-spot"
    BINANCE_FUTURES = "binance-futures"
    TOOBIT_FUTURES = "toobit-futures"


class EventType(str, Enum):
    TRADE = "TRADE"
    TICKER = "TICKER"
    BOOK_TICKER = "BOOK_TICKER"
    ORDERBOOK_SNAPSHOT = "ORDERBOOK_SNAPSHOT"
    ORDERBOOK_DELTA = "ORDERBOOK_DELTA"
    KLINE = "KLINE"
    MARK_PRICE = "MARK_PRICE"
    INDEX_PRICE = "INDEX_PRICE"
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    LIQUIDATION = "LIQUIDATION"
    LONG_SHORT_RATIO = "LONG_SHORT_RATIO"
    CONTRACT_SPEC = "CONTRACT_SPEC"
    SERVER_TIME = "SERVER_TIME"


class Provenance(str, Enum):
    OBSERVED = "OBSERVED"      # taken directly from an exchange message
    DERIVED = "DERIVED"        # computed from observed data (e.g. book imbalance)
    ESTIMATED = "ESTIMATED"    # model/heuristic output, never to be shown as observed


class EnvelopeError(ValueError):
    pass


# Any of these makes an event UNVERIFIED for the quality gate (never READY, never compared across exchanges):
# schema not verified, or a semantic link (contract->index mapping, quantity unit) not officially confirmed.
UNVERIFIED_FLAGS = frozenset({"UNVERIFIED_SCHEMA", "UNVERIFIED_CONTRACT_MAPPING", "UNVERIFIED_UNIT",
                              "UNVERIFIED_AGGREGATION"})
PENALTY_FLAGS = frozenset({"FUTURE_EVENT_TIME", "NO_EVENT_TIME", "CROSSED_BOOK"}) | UNVERIFIED_FLAGS
_BASE = {Provenance.OBSERVED: 1.0, Provenance.DERIVED: 0.8, Provenance.ESTIMATED: 0.5}

# event type -> (positive decimals, non-negative decimals, signed decimals, strings) all REQUIRED
_SPEC = {
    EventType.TRADE: (("price",), ("quantity",), (), ()),
    EventType.TICKER: (("price",), (), (), ()),
    EventType.BOOK_TICKER: (("bid_price", "ask_price"), ("bid_qty", "ask_qty"), (), ()),
    EventType.ORDERBOOK_SNAPSHOT: ((), (), (), ()),
    EventType.ORDERBOOK_DELTA: ((), (), (), ()),
    EventType.KLINE: (("open", "high", "low", "close"), ("volume",), (), ("interval",)),
    EventType.MARK_PRICE: (("mark_price",), (), (), ()),
    EventType.INDEX_PRICE: (("index_price",), (), (), ()),
    EventType.FUNDING_RATE: ((), (), ("funding_rate",), ()),
    EventType.OPEN_INTEREST: ((), ("open_interest",), (), ()),
    EventType.LIQUIDATION: (("price",), ("quantity",), (), ("side",)),
    EventType.LONG_SHORT_RATIO: (("ratio",), (), (), ()),
    EventType.CONTRACT_SPEC: ((), (), (), ()),
    EventType.SERVER_TIME: ((), (), (), ()),
}
_OPTIONAL_DEC = ("index_price", "mark_price", "next_funding_time_ms", "contract_size", "tick_size",
                 "step_size", "edp_10m_avg", "imbalance", "last_price", "quote_volume", "server_time_ms")
_BOOK_TYPES = (EventType.ORDERBOOK_SNAPSHOT, EventType.ORDERBOOK_DELTA)


def to_decimal(v: Any, name: str = "value") -> Decimal:
    if isinstance(v, bool) or v is None:
        raise EnvelopeError(f"{name}: bool/None is not a number")
    if isinstance(v, Decimal):
        d = v
    elif isinstance(v, (int, str)):
        try:
            d = Decimal(v.strip() if isinstance(v, str) else v)
        except InvalidOperation:
            raise EnvelopeError(f"{name}: not a number: {v!r}")
    elif isinstance(v, float):
        if not math.isfinite(v):
            raise EnvelopeError(f"{name}: non-finite {v}")
        d = Decimal(repr(v))
    else:
        raise EnvelopeError(f"{name}: bad type {type(v).__name__}")
    if not d.is_finite():
        raise EnvelopeError(f"{name}: non-finite {v!r}")
    return d


def utc(v: Any, name: str = "time", allow_none: bool = False) -> Optional[datetime]:
    """Aware datetime -> UTC. Naive datetimes are REJECTED (no guessing a timezone)."""
    if v is None:
        if allow_none:
            return None
        raise EnvelopeError(f"{name} is required")
    if isinstance(v, str):
        try:
            v = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise EnvelopeError(f"{name}: invalid datetime {v!r}")
    if not isinstance(v, datetime):
        raise EnvelopeError(f"{name}: bad type {type(v).__name__}")
    if v.tzinfo is None or v.utcoffset() is None:
        raise EnvelopeError(f"{name}: naive datetime rejected (UTC offset required)")
    return v.astimezone(timezone.utc)


def ms_to_utc(ms: Any, name: str = "time") -> datetime:
    d = to_decimal(ms, name)
    if d < 0:
        raise EnvelopeError(f"{name}: negative timestamp")
    return datetime.fromtimestamp(float(d) / 1000.0, tz=timezone.utc)


def _levels(v: Any, name: str, allow_zero_qty: bool):
    if not isinstance(v, (list, tuple)):
        raise EnvelopeError(f"{name}: must be a list of [price, qty]")
    out = []
    for lv in v:
        if not isinstance(lv, (list, tuple)) or len(lv) != 2:
            raise EnvelopeError(f"{name}: bad level {lv!r}")
        p, q = to_decimal(lv[0], name + ".price"), to_decimal(lv[1], name + ".qty")
        if p <= 0 or q < 0 or (q == 0 and not allow_zero_qty):
            raise EnvelopeError(f"{name}: invalid level {lv!r}")
        out.append([p, q])
    return out


def _normalise(et: EventType, np: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(np, dict):
        raise EnvelopeError("normalized_payload must be a dict")
    pos, nonneg, signed, strs = _SPEC[et]
    out: Dict[str, Any] = {}
    for k in pos:
        out[k] = to_decimal(np.get(k), k)
        if out[k] <= 0:
            raise EnvelopeError(f"{k} must be > 0")
    for k in nonneg:
        out[k] = to_decimal(np.get(k), k)
        if out[k] < 0:
            raise EnvelopeError(f"{k} must be >= 0")
    for k in signed:
        out[k] = to_decimal(np.get(k), k)
    for k in strs:
        if not isinstance(np.get(k), str) or not np[k]:
            raise EnvelopeError(f"{k} must be a non-empty string")
        out[k] = np[k]
    for k in _OPTIONAL_DEC:
        if k in np and np[k] is not None:
            out[k] = to_decimal(np[k], k)
            if out[k] < 0 and k != "imbalance":
                raise EnvelopeError(f"{k} must be >= 0")
    if et in _BOOK_TYPES:
        z = et == EventType.ORDERBOOK_DELTA
        out["bids"] = _levels(np.get("bids", []), "bids", z)
        out["asks"] = _levels(np.get("asks", []), "asks", z)
    for k, v in np.items():       # remaining plain metadata (str/bool/None/int only)
        if k in out or k in _OPTIONAL_DEC or k in ("bids", "asks"):
            continue
        if isinstance(v, (str, bool, type(None))) or isinstance(v, int):
            out[k] = v
        else:
            raise EnvelopeError(f"unsupported normalized field {k!r}")
    return out


def _ser(v: Any) -> Any:
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, list):
        return [_ser(x) for x in v]
    if isinstance(v, dict):
        return {k: _ser(x) for k, x in v.items()}
    return v


class MarketEnvelope:
    """Immutable-by-convention market event. Payloads are deep-copied in and out."""

    def __init__(self, *, source, symbol: str, event_type, event_time, received_at, raw_payload,
                 normalized_payload, exchange_symbol: Optional[str] = None,
                 provenance=Provenance.OBSERVED, quality_flags=(), sequence_id: Optional[str] = None,
                 hypothesis_id: Optional[str] = None, schema_version: str = SCHEMA_VERSION,
                 _digest: Optional[str] = None):
        try:
            self._source = Source(source)
        except ValueError:
            raise EnvelopeError(f"unknown source {source!r}")
        try:
            self._et = EventType(event_type)
            self._prov = Provenance(provenance)
        except ValueError as e:
            raise EnvelopeError(str(e))
        if schema_version != SCHEMA_VERSION:
            raise EnvelopeError(f"schema_version mismatch: {schema_version!r} != {SCHEMA_VERSION!r}")
        if not isinstance(symbol, str) or not symbol.strip():
            raise EnvelopeError("symbol must be a non-empty string")
        self._symbol, self._xsym = symbol, exchange_symbol
        self._received_at = utc(received_at, "received_at")
        self._event_time = utc(event_time, "event_time", allow_none=True)
        if not isinstance(raw_payload, (dict, list)):
            raise EnvelopeError("raw_payload must be a dict or list")
        self._raw = copy.deepcopy(raw_payload)
        self._norm = _normalise(self._et, copy.deepcopy(normalized_payload))
        flags = set(quality_flags)
        if self._event_time is None:
            flags.add("NO_EVENT_TIME")
        elif self._event_time > self._received_at + FUTURE_SKEW:
            flags.add("FUTURE_EVENT_TIME")
        self._flags = tuple(sorted(flags))
        self._seq = None if sequence_id is None else str(sequence_id)
        self._hyp = hypothesis_id
        self._score = max(0.0, min(1.0, _BASE[self._prov] - 0.25 * len(flags & PENALTY_FLAGS)))
        d = self._compute_digest()
        if _digest is not None and _digest != d:
            raise EnvelopeError("digest mismatch: envelope content was modified")
        self._digest = d

    source = property(lambda s: s._source)
    symbol = property(lambda s: s._symbol)
    exchange_symbol = property(lambda s: s._xsym)
    event_type = property(lambda s: s._et)
    event_time = property(lambda s: s._event_time)
    received_at = property(lambda s: s._received_at)
    schema_version = property(lambda s: SCHEMA_VERSION)
    reliability_score = property(lambda s: s._score)
    raw_payload = property(lambda s: copy.deepcopy(s._raw))
    normalized_payload = property(lambda s: copy.deepcopy(s._norm))
    hypothesis_id = property(lambda s: s._hyp)
    quality_flags = property(lambda s: s._flags)
    sequence_id = property(lambda s: s._seq)
    provenance = property(lambda s: s._prov)
    digest = property(lambda s: s._digest)

    def _body(self) -> Dict[str, Any]:
        return {
            "source": self._source.value, "symbol": self._symbol, "exchange_symbol": self._xsym,
            "event_type": self._et.value,
            "event_time": self._event_time.isoformat() if self._event_time else None,
            "received_at": self._received_at.isoformat(), "schema_version": SCHEMA_VERSION,
            "reliability_score": self._score, "raw_payload": copy.deepcopy(self._raw),
            "normalized_payload": _ser(self._norm), "hypothesis_id": self._hyp,
            "quality_flags": list(self._flags), "sequence_id": self._seq,
            "provenance": self._prov.value,
        }

    def _compute_digest(self) -> str:
        return hashlib.sha256(json.dumps(self._body(), sort_keys=True, separators=(",", ":"),
                                         default=str).encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        d = self._body()
        d["digest"] = self._digest
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MarketEnvelope":
        if not isinstance(d, dict):
            raise EnvelopeError("envelope must be a dict")
        need = {"source", "symbol", "event_type", "event_time", "received_at", "schema_version",
                "raw_payload", "normalized_payload", "digest"}
        missing = need - set(d)
        if missing:
            raise EnvelopeError(f"missing fields: {sorted(missing)}")
        env = cls(source=d["source"], symbol=d["symbol"], event_type=d["event_type"],
                  event_time=d["event_time"], received_at=d["received_at"], raw_payload=d["raw_payload"],
                  normalized_payload=d["normalized_payload"], exchange_symbol=d.get("exchange_symbol"),
                  provenance=d.get("provenance", "OBSERVED"), quality_flags=d.get("quality_flags", ()),
                  sequence_id=d.get("sequence_id"), hypothesis_id=d.get("hypothesis_id"),
                  schema_version=d["schema_version"], _digest=d["digest"])
        if "reliability_score" in d and d["reliability_score"] != env.reliability_score:
            raise EnvelopeError("reliability_score does not match recomputed value")
        return env

    @classmethod
    def from_json(cls, s: str) -> "MarketEnvelope":
        try:
            return cls.from_dict(json.loads(s))
        except json.JSONDecodeError as e:
            raise EnvelopeError(f"invalid JSON: {e}")

    def __repr__(self):
        return f"MarketEnvelope({self._source.value} {self._symbol} {self._et.value} seq={self._seq})"
