"""③ Retail position change agent. A public long/short ACCOUNT ratio is not the real position of all retail
traders. Without a confirmed population / update period the observation is quarantined (UNVERIFIED)."""
from ...stage0.envelope import Source
from .. import quality_policy as policy
from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent


class RetailPositionChangeAgent(BaseAgent):
    agent_id = "retail_position_change"
    request_types = ("LONG_SHORT_RATIO",)

    def analyze(self, inp: AgentInput) -> Analysis:
        if Source(inp.source) not in policy.FUTURES_SOURCES:
            return Analysis(AgentStatus.DATA_UNAVAILABLE, missing=["long/short ratio does not exist for spot"])
        if not inp.ready("LONG_SHORT_RATIO"):
            return self.unavailable(inp, self.request_types, "LONG_SHORT_RATIO not READY", ["long_short_ratio"])
        obs = sorted(inp.events["LONG_SHORT_RATIO"], key=lambda e: e.event_time or e.received_at)
        lacking = [k for k in policy.LSR_REQUIRED_META if any(not e.normalized_payload.get(k) for e in obs)]
        if lacking:
            return Analysis(AgentStatus.UNVERIFIED, missing=[f"ratio metadata: {k}" for k in lacking],
                            warnings=["Population, unit and update period of the public ratio are not confirmed: "
                                      f"{len(obs)} observation(s) quarantined, no metric computed."])
        first, last = obs[0].normalized_payload, obs[-1].normalized_payload
        m = {"population": last["population"], "period_seconds": last["period_seconds"],
             "observations": len(obs), "latest_ratio": last["ratio"],
             "latest_long_account": last.get("long_account"), "latest_short_account": last.get("short_account"),
             "scope": "published ratio of the stated population; not total retail positioning"}
        missing = []
        if len(obs) >= 2:
            m["ratio_change"] = last["ratio"] - first["ratio"]
        else:
            missing.append("ratio change: needs >=2 observations")
        return Analysis(AgentStatus.READY if not missing else AgentStatus.DEGRADED, m, missing, [],
                        1.0 if not missing else 0.5, "ratio (+ change) x min input reliability",
                        used_types=["LONG_SHORT_RATIO"])
