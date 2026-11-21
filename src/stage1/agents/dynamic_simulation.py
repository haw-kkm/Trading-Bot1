"""⑧ Dynamic simulation agent: OFFLINE what-if arithmetic on verified prices.
It imports nothing from the order engine / accounts, creates no orders and never reports a result as a fill
or a realised return. Fees, slippage, funding and maintenance margin are NOT modelled."""
from decimal import Decimal

from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent
from .funding_technical import ATR_N, MACD_SIG, MACD_SLOW, RSI_N, SMA_N
from .indicators import atr
from .series import select_series

SHOCKS = (Decimal("-0.05"), Decimal("-0.01"), Decimal("0.01"), Decimal("0.05"))
DELAYS_S = (5, 30, 120)
DROPS = (Decimal("0.25"), Decimal("0.5"))
ATR_MULT = Decimal(2)
LABEL = {"simulated": True, "is_real_fill_return": False, "real_orders_created": 0,
         "excluded_from_model": ["fees", "slippage", "funding", "maintenance margin", "partial fills"]}


class DynamicSimulationAgent(BaseAgent):
    agent_id = "dynamic_simulation"
    request_types = ("TRADE", "KLINE")

    def __init__(self, notional: Decimal = Decimal("1000"), leverage: int = 10, trade_bar_seconds: int = 60):
        self.notional, self.leverage, self.trade_bar_seconds = Decimal(notional), int(leverage), trade_bar_seconds

    @staticmethod
    def _pnl(qty, entry, exit_, long: bool):
        return qty * ((exit_ - entry) if long else (entry - exit_))

    def analyze(self, inp: AgentInput) -> Analysis:
        series = select_series(inp, self.trade_bar_seconds, ATR_N + 1)
        if series is None:
            return self.unavailable(inp, self.request_types, "no READY price series", ["price"])
        trades = inp.events.get("TRADE", ())
        entry = trades[-1].normalized_payload["price"] if trades else series.bars[-1].close
        qty = self.notional / entry
        missing, warns = [], []
        scen = {"entry_price": entry, "notional_quote": self.notional, "quantity_base": qty,
                "price_shocks": [], **LABEL}
        for s in SHOCKS:
            px = entry * (1 + s)
            scen["price_shocks"].append({"shock_pct": s * 100, "price": px,
                                         "hypothetical_long_pnl_quote": self._pnl(qty, entry, px, True),
                                         "hypothetical_short_pnl_quote": self._pnl(qty, entry, px, False)})
        scen["toy_liquidation_model"] = {
            "leverage": self.leverage, "model": "ESTIMATED: entry x (1 -/+ 1/leverage), no maintenance margin",
            "long_liq_price": entry * (1 - Decimal(1) / self.leverage),
            "short_liq_price": entry * (1 + Decimal(1) / self.leverage)}
        a = atr(series.bars, ATR_N)
        if a is not None:
            lo, hi = entry - ATR_MULT * a, entry + ATR_MULT * a
            scen["volatility_expansion"] = {"atr": a, "multiplier": ATR_MULT, "low": lo, "high": hi,
                                            "hypothetical_long_pnl_at_low": self._pnl(qty, entry, lo, True),
                                            "hypothetical_short_pnl_at_high": self._pnl(qty, entry, hi, False)}
        else:
            missing.append(f"volatility_expansion: ATR needs {ATR_N + 1} bars, have {len(series.bars)}")
        delay = []
        for t in ("TRADE", "KLINE"):
            if inp.ready(t) and inp.events.get(t):
                age = (inp.now - max(e.received_at for e in inp.events[t])).total_seconds()
                lim = inp.max_age[t].total_seconds()
                for d in DELAYS_S:
                    delay.append({"event_type": t, "extra_delay_seconds": d, "age_seconds": age + d,
                                  "max_age_seconds": lim,
                                  "outcome": "WOULD_BE_STALE" if age + d > lim else "STILL_FRESH"})
        scen["data_delay"] = delay
        n = len(series.bars)
        need = {f"sma_{SMA_N}": SMA_N, f"rsi_{RSI_N}": RSI_N + 1, "macd": MACD_SLOW + MACD_SIG - 1,
                f"atr_{ATR_N}": ATR_N + 1}
        scen["data_missing"] = [{"dropped_fraction": f, "bars_left": int(n * (1 - f)),
                                 "lost_indicators": sorted(k for k, v in need.items() if int(n * (1 - f)) < v)}
                                for f in DROPS]
        if series.origin != "KLINE":
            warns.append("DEGRADED: bars are trade-derived (exchange candles unavailable)")
        if series.note:
            warns.append(series.note)
        warns.append("Simulation output: not an executed fill, not a realised return, not a forecast.")
        st = AgentStatus.READY if not missing and series.origin == "KLINE" else AgentStatus.DEGRADED
        used = ["KLINE" if series.origin == "KLINE" else "TRADE"] + (["TRADE"] if trades and series.origin == "KLINE" else [])
        return Analysis(st, {"series": {"origin": series.origin, "bars": n}, "scenarios": scen}, missing, warns,
                        1.0 if not missing else 0.7, "scenario groups computed x min input reliability",
                        reliability_floor=series.reliability, used_types=used)
