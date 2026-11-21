"""⑥ Funding & technical-indicator agent. Each indicator is computed only with its minimum history."""
from decimal import Decimal

from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent
from .indicators import atr, macd, returns, rsi, sma, stdev
from .series import select_series

RSI_N, ATR_N, SMA_N, VOL_N = 14, 14, 20, 20
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
FUNDING_N = 8
MIN_BARS = MACD_SLOW + MACD_SIG - 1                         # longest requirement (MACD) = 34 closes


class FundingTechnicalAgent(BaseAgent):
    agent_id = "funding_technical"
    request_types = ("FUNDING_RATE", "TRADE", "KLINE")

    def __init__(self, trade_bar_seconds: int = 60):
        self.trade_bar_seconds = trade_bar_seconds

    def analyze(self, inp: AgentInput) -> Analysis:
        m, missing, warns, used = {}, [], [], []
        total = got = 0
        # ---- funding ------------------------------------------------------------------------
        total += 2
        if inp.ready("FUNDING_RATE"):
            fr = sorted(inp.events["FUNDING_RATE"], key=lambda e: e.event_time or e.received_at)
            rates = [e.normalized_payload["funding_rate"] for e in fr]
            m["funding_rate_latest"] = rates[-1]
            m["funding_observations"] = len(rates)
            got += 1
            if len(rates) >= 2:
                w = rates[-FUNDING_N:]
                m["funding_rate_mean"] = sum(w, Decimal(0)) / len(w)
                m["funding_mean_window"] = len(w)
                got += 1
            else:
                missing.append("funding_rate_mean: needs >=2 funding observations")
            used.append("FUNDING_RATE")
            warns.append("funding interval is not in the envelope: no annualisation is reported")
        else:
            missing.append(f"funding: FUNDING_RATE is {inp.readiness.get('FUNDING_RATE', 'NOT_REQUESTED')}")
        # ---- technicals ---------------------------------------------------------------------
        total += 6
        series = select_series(inp, self.trade_bar_seconds, MIN_BARS)
        origin = None
        if series is None:
            missing.append("technicals: no READY price series (KLINE or TRADE)")
        else:
            origin = series.origin
            used.append("KLINE" if origin == "KLINE" else "TRADE")
            bars, closes = series.bars, [b.close for b in series.bars]
            m["price_source"], m["bar_interval"], m["bar_count"] = origin, series.interval, len(bars)
            def put(name, val, need):
                nonlocal got
                if val is None:
                    missing.append(f"{name}: needs {need} bars, have {len(bars)}")
                else:
                    m[name] = val
                    got += 1
            put(f"sma_{SMA_N}", sma(closes, SMA_N), SMA_N)
            put(f"rsi_{RSI_N}", rsi(closes, RSI_N), RSI_N + 1)
            mc = macd(closes, MACD_FAST, MACD_SLOW, MACD_SIG)
            put("macd", None if mc is None else mc["macd"], MIN_BARS)
            if mc:
                m["macd_signal"], m["macd_histogram"] = mc["signal"], mc["histogram"]
            put(f"atr_{ATR_N}", atr(bars, ATR_N), ATR_N + 1)
            put(f"volatility_{VOL_N}", stdev(returns(closes[-(VOL_N + 1):])) if len(closes) > VOL_N else None,
                VOL_N + 1)
            if series.note:
                warns.append(series.note)
            if origin != "KLINE":
                warns.append("DEGRADED: bars are trade-derived (exchange candles unavailable)")
        if got == 0:
            return Analysis(AgentStatus.DATA_UNAVAILABLE, missing=missing,
                            warnings=warns + ["no funding and no indicator could be computed"])
        status = AgentStatus.READY if (not missing and origin == "KLINE") else AgentStatus.DEGRADED
        return Analysis(status, m, missing, warns, got / total,
                        "indicators computed / expected x min input reliability",
                        reliability_floor=series.reliability if series else None, used_types=used)
