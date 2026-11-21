"""② Whale-position agent. Public data cannot reveal any account's position: this agent only reports
OBSERVED large prints. Entry price, leverage and position size of a whale are never produced."""
from decimal import Decimal

from ...stage0.envelope import Source
from .. import quality_policy as policy
from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent


class WhalePositionAgent(BaseAgent):
    agent_id = "whale_position"
    request_types = ("TRADE",)

    def __init__(self, large_trade_notional: Decimal = Decimal("100000")):
        self.threshold = Decimal(large_trade_notional)

    def analyze(self, inp: AgentInput) -> Analysis:
        src = Source(inp.source)
        if src not in policy.NOTIONAL_VERIFIED_SOURCES:
            return Analysis(AgentStatus.UNVERIFIED, missing=["trade quantity unit unconfirmed for notional"],
                            warnings=[f"{src.value}: trade quantity unit not confirmed as base asset; "
                                      "notional (price x quantity) would be unverified"])
        if not inp.ready("TRADE"):
            return self.unavailable(inp, self.request_types, "TRADE not READY: no observable input", ["TRADE"])
        large, buy, sell, unknown, biggest = 0, Decimal(0), Decimal(0), Decimal(0), Decimal(0)
        for e in inp.events["TRADE"]:
            n = e.normalized_payload
            notional = n["price"] * n["quantity"]
            if notional < self.threshold:
                continue
            large += 1
            biggest = max(biggest, notional)
            if isinstance(n.get("buyer_is_maker"), bool):
                if n["buyer_is_maker"]:
                    sell += notional                      # buyer is maker -> the aggressor sold
                else:
                    buy += notional
            elif isinstance(n.get("is_buy"), bool):
                if n["is_buy"]:
                    buy += notional
                else:
                    sell += notional
            else:
                unknown += notional
        m = {"observed_large_trades": {"threshold_notional_quote": self.threshold, "count": large,
                                       "aggressive_buy_notional": buy, "aggressive_sell_notional": sell,
                                       "unknown_side_notional": unknown, "largest_notional": biggest,
                                       "trades_scanned": len(inp.events["TRADE"])},
             "public_large_account_statistics": "NOT_COLLECTED",
             "estimated_whale_activity": "NOT_ESTIMATED",
             "actual_account_positions": "UNAVAILABLE"}
        return Analysis(AgentStatus.DEGRADED, m,
                        ["whale account positions (entry, leverage, size)", "public top-trader position statistics"],
                        ["Observed large prints only. A large print is not a whale position; "
                         "real whale positions cannot be known from public data."],
                        0.5, "observed large-trade scan only (position-level inputs absent) x min input reliability",
                        used_types=["TRADE"])
