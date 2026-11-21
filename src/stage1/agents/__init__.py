"""Registry of the 8 Stage 1 agents (fixed order = design order ①..⑧)."""
from .dynamic_simulation import DynamicSimulationAgent
from .fear_greed import FearGreedAgent
from .funding_technical import FundingTechnicalAgent
from .liquidation_map import LiquidationMapAgent
from .open_interest import OpenInterestAgent
from .retail_position_change import RetailPositionChangeAgent
from .trend import TrendAgent
from .whale_position import WhalePositionAgent

AGENT_CLASSES = (TrendAgent, WhalePositionAgent, RetailPositionChangeAgent, LiquidationMapAgent,
                 OpenInterestAgent, FundingTechnicalAgent, FearGreedAgent, DynamicSimulationAgent)


def build_agents():
    return [c() for c in AGENT_CLASSES]
