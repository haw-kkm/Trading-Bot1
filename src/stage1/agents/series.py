"""Shared bar-series selection: exchange KLINE when READY and long enough, else bars derived from TRADE."""
from typing import Optional

from ..models import AgentInput
from .indicators import BarSeries, bars_from_klines, bars_from_trades


def select_series(inp: AgentInput, trade_bar_seconds: int, min_bars: int) -> Optional[BarSeries]:
    cands = []
    if inp.ready("KLINE"):
        s = bars_from_klines(inp.events.get("KLINE", ()))
        if s:
            cands.append(s)
    if inp.ready("TRADE"):
        s = bars_from_trades(inp.events.get("TRADE", ()), inp.now, trade_bar_seconds)
        if s:
            cands.append(s)
    if not cands:
        return None
    ok = [s for s in cands if len(s.bars) >= min_bars]
    if ok:
        return next((s for s in ok if s.origin == "KLINE"), ok[0])
    return max(cands, key=lambda s: len(s.bars))
