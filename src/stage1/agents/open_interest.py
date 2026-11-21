"""⑤ Open Interest agent. Per-source, never cross-exchange. Quarantines sources with unverified aggregation."""
from decimal import Decimal

from ...stage0.envelope import Source
from .. import quality_policy as policy
from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent


class OpenInterestAgent(BaseAgent):
    agent_id = "open_interest"
    request_types = ("OPEN_INTEREST",)

    def analyze(self, inp: AgentInput) -> Analysis:
        src = Source(inp.source)
        if src not in policy.FUTURES_SOURCES:
            return Analysis(AgentStatus.DATA_UNAVAILABLE, missing=["open interest does not exist for spot"],
                            warnings=["spot market has no open interest"])
        notes = [policy.TOOBIT_OI_NOTE] if src == Source.TOOBIT_FUTURES else []
        if not policy.OI_AGGREGATION_VERIFIED.get(src, False):
            # even if the gate is fed a clean-looking OI event for an unverified-aggregation source, refuse it
            return Analysis(AgentStatus.UNVERIFIED, missing=["OI aggregation basis unverified"],
                            warnings=notes or ["OI aggregation basis unverified for this source"])
        if not inp.ready("OPEN_INTEREST"):
            a = self.unavailable(inp, self.request_types, "OPEN_INTEREST not READY", ["open_interest"])
            a.warnings += notes
            return a
        obs = sorted(inp.events["OPEN_INTEREST"], key=lambda e: e.event_time or e.received_at)
        vals = [e.normalized_payload["open_interest"] for e in obs]
        m = {"latest_open_interest": vals[-1], "unit": "BASE_ASSET", "observations": len(vals),
             "cross_exchange_comparison": "NOT_PERFORMED",
             "latest_event_time": (obs[-1].event_time or obs[-1].received_at).isoformat()}
        missing = []
        if len(vals) >= 2 and vals[0] != 0:
            m["change_abs"] = vals[-1] - vals[0]
            m["change_pct"] = (vals[-1] - vals[0]) / vals[0] * 100
            m["change_window_start"] = (obs[0].event_time or obs[0].received_at).isoformat()
        else:
            missing.append("OI change: needs >=2 observations with non-zero first value")
        st = AgentStatus.READY if not missing else AgentStatus.DEGRADED
        return Analysis(st, m, missing, notes, 1.0 if not missing else 0.5,
                        "level (+ change when >=2 observations) x min input reliability",
                        used_types=["OPEN_INTEREST"])
