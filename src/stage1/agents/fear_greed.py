"""⑦ Fear & Greed agent. No trusted public source is verified in Stage 0 and no external API may be added
or authenticated here: always DATA_UNAVAILABLE."""
from ..models import AgentInput, AgentStatus
from .base import Analysis, BaseAgent


class FearGreedAgent(BaseAgent):
    agent_id = "fear_greed"
    request_types = ()

    def analyze(self, inp: AgentInput) -> Analysis:
        return Analysis(AgentStatus.DATA_UNAVAILABLE, missing=["fear & greed index source"],
                        warnings=["No verified public fear & greed source (not part of Stage 0). "
                                  "No external API is called by this agent."])
