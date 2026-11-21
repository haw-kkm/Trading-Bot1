"""① Trend agent: direction / strength description of the observed past. NOT a forecast."""
from decimal import Decimal

from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent
from .indicators import efficiency_ratio, returns, sma, stdev
from .series import select_series

SHORT, LONG, MOM, VOL_N, VOLCHG_N = 5, 20, 10, 20, 5
MIN_BARS = LONG                                              # smallest series that yields the full core


class TrendAgent(BaseAgent):
    agent_id = "trend"
    request_types = ("TRADE", "KLINE")

    def __init__(self, trade_bar_seconds: int = 60):
        self.trade_bar_seconds = trade_bar_seconds

    def analyze(self, inp: AgentInput) -> Analysis:
        series = select_series(inp, self.trade_bar_seconds, MIN_BARS)
        if series is None:
            return self.unavailable(inp, self.request_types, "no READY price series (KLINE or TRADE)",
                                    ["price bars"])
        bars = series.bars
        closes = [b.close for b in bars]
        vols = [b.volume for b in bars]
        m, missing = {}, []
        m["price_source"] = series.origin
        m["bar_interval"] = series.interval
        m["bar_count"] = len(bars)
        m["last_close"] = closes[-1]
        if len(closes) >= 2:
            m["price_change_pct"] = (closes[-1] - closes[0]) / closes[0] * 100
        else:
            missing.append("price_change_pct: needs >=2 bars")
        s, l = sma(closes, SHORT), sma(closes, LONG)
        if s is not None: m[f"sma_{SHORT}"] = s
        else: missing.append(f"sma_{SHORT}: needs {SHORT} bars, have {len(closes)}")
        if l is not None: m[f"sma_{LONG}"] = l
        else: missing.append(f"sma_{LONG}: needs {LONG} bars, have {len(closes)}")
        if len(closes) > MOM:
            m[f"momentum_{MOM}"] = closes[-1] - closes[-1 - MOM]
        else:
            missing.append(f"momentum_{MOM}: needs {MOM + 1} bars, have {len(closes)}")
        if len(vols) >= 2 * VOLCHG_N:
            recent, prior = sum(vols[-VOLCHG_N:], Decimal(0)), sum(vols[-2 * VOLCHG_N:-VOLCHG_N], Decimal(0))
            if prior > 0:
                m["volume_change_pct"] = (recent - prior) / prior * 100
            else:
                missing.append("volume_change_pct: prior-window volume is 0")
        else:
            missing.append(f"volume_change_pct: needs {2 * VOLCHG_N} bars, have {len(vols)}")
        sd = stdev(returns(closes[-(VOL_N + 1):])) if len(closes) >= VOL_N + 1 else None
        if sd is not None: m[f"volatility_{VOL_N}"] = sd
        else: missing.append(f"volatility_{VOL_N}: needs {VOL_N + 1} bars, have {len(closes)}")
        er = efficiency_ratio(closes, MOM)
        if er is not None: m["trend_strength"] = er
        else: missing.append(f"trend_strength: needs {MOM + 1} bars")
        if s is not None and l is not None and MOM_OK(m):
            up, dn = s > l and m[f"momentum_{MOM}"] > 0, s < l and m[f"momentum_{MOM}"] < 0
            m["direction"] = "UP" if up else "DOWN" if dn else "SIDEWAYS"
        else:
            missing.append("direction: needs sma_short, sma_long and momentum")
        m["disclaimer"] = "Descriptive statistics of past bars; not a price forecast."
        expected = 8
        got = sum(1 for k in ("price_change_pct", f"sma_{SHORT}", f"sma_{LONG}", f"momentum_{MOM}",
                              "volume_change_pct", f"volatility_{VOL_N}", "trend_strength", "direction") if k in m)
        warns = [series.note] if series.note else []
        if got < 2:
            return Analysis(AgentStatus.DATA_UNAVAILABLE, missing=missing + ["insufficient bars"],
                            warnings=warns + ["time series too short for any trend metric"])
        status = AgentStatus.READY if (not missing and series.origin == "KLINE") else AgentStatus.DEGRADED
        if series.origin != "KLINE":
            warns.append("DEGRADED: bars are trade-derived (exchange candles unavailable)")
        return Analysis(status, m, missing, warns, got / expected, "metrics computed / 8 x min input reliability",
                        reliability_floor=series.reliability, used_types=[
                            "KLINE" if series.origin == "KLINE" else "TRADE"])


def MOM_OK(m):
    return f"momentum_{MOM}" in m
