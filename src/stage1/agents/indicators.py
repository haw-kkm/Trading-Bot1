"""Pure Decimal indicator math + bar construction. No I/O, no clock, no floats (precision is preserved)."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple

from ...stage0.envelope import MarketEnvelope
from ..quality_policy import event_instant

ZERO = Decimal(0)


@dataclass(frozen=True)
class Bar:
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass
class BarSeries:
    bars: List[Bar]
    origin: str                      # "KLINE" (exchange candles) | "TRADE_DERIVED"
    interval: str
    reliability: float
    note: str = ""


def sma(xs: Sequence[Decimal], n: int) -> Optional[Decimal]:
    if n <= 0 or len(xs) < n:
        return None
    return sum(xs[-n:], ZERO) / n


def ema_series(xs: Sequence[Decimal], n: int) -> List[Decimal]:
    """EMA seeded with the SMA of the first n values; len = len(xs) - n + 1 (empty if too short)."""
    if len(xs) < n:
        return []
    k = Decimal(2) / (n + 1)
    out = [sum(xs[:n], ZERO) / n]
    for x in xs[n:]:
        out.append((x - out[-1]) * k + out[-1])
    return out


def rsi(closes: Sequence[Decimal], n: int = 14) -> Optional[Decimal]:
    """Wilder RSI; needs n+1 closes."""
    if len(closes) < n + 1:
        return None
    d = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    ag = sum((x for x in d[:n] if x > 0), ZERO) / n
    al = sum((-x for x in d[:n] if x < 0), ZERO) / n
    for x in d[n:]:
        ag = (ag * (n - 1) + (x if x > 0 else ZERO)) / n
        al = (al * (n - 1) + (-x if x < 0 else ZERO)) / n
    if al == 0:
        return Decimal(100) if ag > 0 else Decimal(50)
    return Decimal(100) - Decimal(100) / (1 + ag / al)


def macd(closes: Sequence[Decimal], fast=12, slow=26, signal=9) -> Optional[Dict[str, Decimal]]:
    """Needs slow + signal - 1 closes."""
    if len(closes) < slow + signal - 1:
        return None
    ef, es = ema_series(closes, fast), ema_series(closes, slow)
    line = [f - s for f, s in zip(ef[len(ef) - len(es):], es)]
    sig = ema_series(line, signal)
    if not sig:
        return None
    return {"macd": line[-1], "signal": sig[-1], "histogram": line[-1] - sig[-1]}


def atr(bars: Sequence[Bar], n: int = 14) -> Optional[Decimal]:
    """Wilder ATR; needs n+1 bars (true range uses the previous close)."""
    if len(bars) < n + 1:
        return None
    tr = [max(b.high - b.low, abs(b.high - p.close), abs(b.low - p.close)) for p, b in zip(bars, bars[1:])]
    a = sum(tr[:n], ZERO) / n
    for t in tr[n:]:
        a = (a * (n - 1) + t) / n
    return a


def returns(closes: Sequence[Decimal]) -> List[Decimal]:
    return [(b - a) / a for a, b in zip(closes, closes[1:])]


def stdev(xs: Sequence[Decimal]) -> Optional[Decimal]:
    """Sample standard deviation (n-1)."""
    if len(xs) < 2:
        return None
    m = sum(xs, ZERO) / len(xs)
    return (sum(((x - m) ** 2 for x in xs), ZERO) / (len(xs) - 1)).sqrt()


def efficiency_ratio(closes: Sequence[Decimal], n: int) -> Optional[Decimal]:
    """Kaufman efficiency ratio in [0,1]: 1 = straight-line move, 0 = pure chop. Needs n+1 closes."""
    if len(closes) < n + 1:
        return None
    w = closes[-(n + 1):]
    path = sum((abs(b - a) for a, b in zip(w, w[1:])), ZERO)
    return ZERO if path == 0 else abs(w[-1] - w[0]) / path


# ---- bars ------------------------------------------------------------------------------------------
def _interval_seconds(label: str) -> Optional[int]:
    unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(label[-1:])
    try:
        return int(label[:-1]) * unit if unit else None
    except ValueError:
        return None


def bars_from_klines(klines: Sequence[MarketEnvelope]) -> Optional[BarSeries]:
    """Exchange candles. Only CLOSED bars (is_closed True) count; a feed that does not say whether a bar
    is closed (Toobit) loses its newest bar per interval (it may still be forming)."""
    by_iv: Dict[str, Dict[int, Tuple[MarketEnvelope, dict]]] = {}
    for e in klines:
        n = e.normalized_payload
        t0 = n.get("open_time_ms")
        key = int(t0) if t0 is not None else int(event_instant(e).timestamp() * 1000)
        by_iv.setdefault(n["interval"], {})[key] = (e, n)          # last update of a bar wins
    best = None
    for iv, rows in by_iv.items():
        items = sorted(rows.items())
        if all("is_closed" in n for _, (_, n) in items):
            items = [(k, v) for k, v in items if v[1]["is_closed"] is True]
        else:
            items = items[:-1]
        if best is None or len(items) > len(best[1]):
            best = (iv, items)
    if best is None or not best[1]:
        return None
    iv, items = best
    bars = [Bar(datetime.fromtimestamp(k / 1000, tz=timezone.utc), n["open"], n["high"], n["low"], n["close"],
                n["volume"]) for k, (_, n) in items]
    return BarSeries(bars, "KLINE", iv, min(e.reliability_score for _, (e, _) in items))


def bars_from_trades(trades: Sequence[MarketEnvelope], now: datetime, seconds: int) -> Optional[BarSeries]:
    """Candles synthesised from observed trades (DERIVED, never presented as exchange candles).
    Only COMPLETE buckets (bucket end <= now) are used. Buckets without trades are skipped, not interpolated."""
    if seconds <= 0 or not trades:
        return None
    buckets: Dict[int, List[Tuple[datetime, Decimal, Decimal]]] = {}
    for e in trades:
        t = event_instant(e)
        n = e.normalized_payload
        buckets.setdefault(int(t.timestamp()) // seconds, []).append((t, n["price"], n["quantity"]))
    now_s = int(now.timestamp())
    bars = []
    for b in sorted(buckets):
        if (b + 1) * seconds > now_s:
            continue
        rows = sorted(buckets[b], key=lambda r: r[0])
        px = [r[1] for r in rows]
        bars.append(Bar(datetime.fromtimestamp(b * seconds, tz=timezone.utc), px[0], max(px), min(px), px[-1],
                        sum((r[2] for r in rows), ZERO)))
    if not bars:
        return None
    rel = min(e.reliability_score for e in trades) * 0.8
    return BarSeries(bars, "TRADE_DERIVED", f"{seconds}s", rel,
                     "bars synthesised from websocket trades; gaps in the trade feed cannot be detected")
