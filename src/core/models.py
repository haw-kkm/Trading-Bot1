"""Core data models for Multi-Agent Cryptocurrency Trading System.

This module implements the foundational data structures for the trading system,
including AgentEnvelope schema and related data types.
"""

import json
import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


class SourceId(str, Enum):
    BINANCE_SPOT = "binance-spot"
    BINANCE_FUTURES = "binance-futures"
    TOOBIT = "toobit"


class AgentEnvelope:
    """Schema for wrapping data from multiple sources with validation.

    This schema standardizes data from Binance Spot, Binance Futures, and Toobit,
    providing structure, validation, and metadata for all trade data.
    """

    def __init__(self, data: Dict[str, Any]):
        """Initialize AgentEnvelope with data.

        Args:
            data: Raw data dictionary to be wrapped and validated

        Raises:
            ValueError: If data fails schema validation
        """
        self._validate_schema(data)
        self.data = data
        self.received_at = datetime.now(timezone.utc).isoformat()
        self.source_id = self._determine_source(data)
        self.reliability_score = self._calculate_reliability(data)
        self.hypothesis_id = data.get("hypothesis_id")

    def _validate_schema(self, data: Dict[str, Any]) -> None:
        """Validate data against AgentEnvelope schema requirements.

        Requirements:
        1. Must contain required fields: timestamp, price, quantity, symbol
        2. Must have valid source identifier
        3. Must have reasonable values (positive numbers, valid formats)
        """
        required_fields = {"timestamp", "price", "quantity", "symbol"}

        # Check required fields exist
        missing_fields = required_fields - set(data.keys())
        if missing_fields:
            raise ValueError(
                f"Missing required fields: {', '.join(missing_fields)}. "
                f"Required fields are: {', '.join(required_fields)}"
            )

        # Validate field types and values
        self._validate_fields(data)

    def _validate_fields(self, data: Dict[str, Any]) -> None:
        """Validate individual field types and values."""
        # Validate timestamp
        timestamp = data["timestamp"]
        if not isinstance(timestamp, str) or not timestamp:
            raise ValueError("timestamp must be a non-empty string")

        # Validate price (must be positive number)
        price = data["price"]
        if not isinstance(price, (int, float)) or price <= 0:
            raise ValueError(f"price must be a positive number, got: {price}")

        # Validate quantity (must be positive number)
        quantity = data["quantity"]
        if not isinstance(quantity, (int, float)) or quantity <= 0:
            raise ValueError(f"quantity must be a positive number, got: {quantity}")

        # Validate symbol (must be non-empty string)
        symbol = data["symbol"]
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(f"symbol must be a non-empty string, got: {symbol}")

    def _determine_source(self, data: Dict[str, Any]) -> str:
        """Determine data source from raw data."""
        if "source" in data:
            source = data["source"]
            if source in [s.value for s in SourceId]:
                return source
        return "unknown"

    def _calculate_reliability(self, data: Dict[str, Any]) -> float:
        """Calculate reliability score based on data completeness and validity."""
        score = 0.0
        total_possible = 0.0

        required_fields = {"timestamp", "price", "quantity", "symbol"}
        present_fields = required_fields & set(data.keys())
        score += len(present_fields) * 0.25
        total_possible += 1.0

        if self._validate_timestamp(data.get("timestamp", "")):
            score += 0.25
        total_possible += 0.5

        if self._validate_price(data.get("price")):
            score += 0.25
        total_possible += 0.5

        if self._validate_quantity(data.get("quantity")):
            score += 0.25
        total_possible += 0.5

        return score / total_possible if total_possible > 0 else 0.0

    def _validate_timestamp(self, timestamp: str) -> bool:
        """Validate timestamp format."""
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return True
        except (ValueError, AttributeError):
            return False

    def _validate_price(self, price: Any) -> bool:
        """Validate price is reasonable."""
        if not isinstance(price, (int, float)):
            return False
        return 0 < price < 1000000

    def _validate_quantity(self, quantity: Any) -> bool:
        """Validate quantity is reasonable."""
        if not isinstance(quantity, (int, float)):
            return False
        return 0 < quantity < 1000000

    def to_dict(self) -> Dict[str, Any]:
        """Convert AgentEnvelope to dictionary for serialization."""
        return {
            "data": self.data,
            "received_at": self.received_at,
            "source_id": self.source_id,
            "reliability_score": self.reliability_score,
            "hypothesis_id": self.hypothesis_id
        }

    def to_json(self) -> str:
        """Convert AgentEnvelope to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentEnvelope":
        """Create AgentEnvelope from dictionary."""
        return cls(data)

    @classmethod
    def from_json(cls, json_str: str) -> "AgentEnvelope":
        """Create AgentEnvelope from JSON string."""
        data = json.loads(json_str)
        return cls(data)


@dataclass(frozen=True)
class TradeHypothesis:
    """Represents a trading hypothesis generated by an agent."""
    symbol: str
    direction: str  # "BUY" or "SELL"
    quantity: float
    leverage: float
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    hypothesis_id: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert TradeHypothesis to dictionary."""
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "quantity": self.quantity,
            "leverage": self.leverage,
            "price": self.price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "hypothesis_id": self.hypothesis_id,
            "timestamp": self.timestamp
        }


@dataclass(frozen=True)
class ValidatedOrder:
    """Immutable authorization token issued by RiskEngine.

    This is the ONLY object that can authorize an ExecutionEngine order.
    It is intentionally frozen so that nothing downstream can mutate the
    clamped quantity, leverage, or signature after issuance.

    The ``signature`` is an HMAC-SHA256 over the canonical form of every
    field that matters for execution, keyed with a secret held only by
    RiskEngine. Verification goes through ``RiskEngine.verify_token``.
    """

    symbol: str
    side: str
    quantity: float          # clamped quantity (baked in by RiskEngine)
    leverage: float
    price: Optional[float]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    hypothesis_id: Optional[str]
    status: str              # "APPROVED" or "CLAMPED"
    original_quantity: float
    signature: str

    @staticmethod
    def canonical_payload(
        symbol, side, quantity, leverage, price, stop_loss, take_profit,
        hypothesis_id, status, original_quantity,
    ) -> bytes:
        """Canonical byte form of the execution-critical fields (to be MAC'd).

        Signing and verification live exclusively in RiskEngine, which owns the
        secret key. This module deliberately exposes no way to sign.
        """
        return json.dumps(
            {
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "leverage": leverage,
                "price": price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "hypothesis_id": hypothesis_id,
                "status": status,
                "original_quantity": original_quantity,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def payload(self) -> bytes:
        """Canonical bytes of this token's fields (excluding the signature)."""
        return self.canonical_payload(
            self.symbol, self.side, self.quantity, self.leverage, self.price,
            self.stop_loss, self.take_profit, self.hypothesis_id, self.status,
            self.original_quantity,
        )
