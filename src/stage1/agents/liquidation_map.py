"""④ Liquidation map agent. Separates OBSERVED liquidations from MODELLED levels and UNAVAILABLE data.
A full exchange heat-map needs exchange-wide positions, which public feeds do not provide: it is withheld."""
from decimal import Decimal

from ...stage0.envelope import Source
from .. import quality_policy as policy
from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent


class LiquidationMapAgent(BaseAgent):
    agent_id = "liquidation_map"
    request_types = ("LIQUIDATION",)

    def analyze(self, inp: AgentInput) -> Analysis:
        if Source(inp.source) not in policy.FUTURES_SOURCES:
            return Analysis(AgentStatus.DATA_UNAVAILABLE, missing=["liquidations do not exist for spot"])
        if not inp.ready("LIQUIDATION"):
            return self.unavailable(inp, self.request_types,
                                    "no READY liquidation events (feed absent or silent: cannot tell which)",
                                    ["LIQUIDATION"])
        long_n = short_n = Decimal(0)
        long_c = short_c = 0
        top = []
        for e in inp.events["LIQUIDATION"]:
            n = e.normalized_payload
            notional = n["price"] * n["quantity"]
            side = str(n["side"]).upper()
            if side == "SELL":                    # a forced SELL closes a LONG
                long_n, long_c = long_n + notional, long_c + 1
            elif side == "BUY":
                short_n, short_c = short_n + notional, short_c + 1
            else:
                continue
            top.append({"price": n["price"], "quantity": n["quantity"], "notional": notional,
                        "liquidated_side": "LONG" if side == "SELL" else "SHORT",
                        "event_time": (e.event_time or e.received_at).isoformat(), "digest": e.digest})
        top.sort(key=lambda r: (-r["notional"], r["event_time"], r["digest"]))
        m = {"observed": {"long_liquidations": {"count": long_c, "notional": long_n},
                          "short_liquidations": {"count": short_c, "notional": short_n},
                          "largest": top[:5],
                          "note": "public liquidation stream; the exchange may throttle/sample forced orders"},
             "modeled": "NONE (no estimated liquidation price bands were produced)",
             "heatmap": "WITHHELD: needs exchange-wide position / leverage data that public feeds do not provide",
             "unavailable": ["exchange-wide open positions by price", "leverage distribution"]}
        return Analysis(AgentStatus.DEGRADED, m, ["liquidation heat-map inputs"],
                        ["Observed liquidations only; no heat-map is generated."], 0.4,
                        "observed events only (heat-map withheld) x min input reliability",
                        used_types=["LIQUIDATION"])
