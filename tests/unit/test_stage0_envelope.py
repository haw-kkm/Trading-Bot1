"""Unit tests for Stage 0 AgentEnvelope and TradeHypothesis models."""

import pytest
from datetime import datetime
from src.core.models import AgentEnvelope, TradeHypothesis, SourceId


def test_agent_envelope_creation_success():
    raw_data = {
        "timestamp": "2026-03-31T12:00:00Z",
        "price": 50000.0,
        "quantity": 1.5,
        "symbol": "BTCUSDT",
        "source": "binance-spot",
        "hypothesis_id": "hypo-123"
    }

    envelope = AgentEnvelope(raw_data)

    assert envelope.data == raw_data
    assert envelope.source_id == "binance-spot"
    assert envelope.hypothesis_id == "hypo-123"
    assert envelope.reliability_score > 0.0
    assert "received_at" in envelope.to_dict()


def test_agent_envelope_missing_required_fields():
    incomplete_data = {
        "timestamp": "2026-03-31T12:00:00Z",
        "price": 50000.0,
        # missing quantity and symbol
    }

    with pytest.raises(ValueError, match="Missing required fields"):
        AgentEnvelope(incomplete_data)


def test_agent_envelope_invalid_field_values():
    invalid_price_data = {
        "timestamp": "2026-03-31T12:00:00Z",
        "price": -100.0, # Negative price
        "quantity": 1.0,
        "symbol": "BTCUSDT"
    }

    with pytest.raises(ValueError, match="price must be a positive number"):
        AgentEnvelope(invalid_price_data)


def test_trade_hypothesis_creation():
    hypo = TradeHypothesis(
        symbol="BTCUSDT",
        direction="BUY",
        quantity=0.5,
        leverage=5.0,
        price=50000.0,
        hypothesis_id="hypo-999"
    )

    data = hypo.to_dict()
    assert data["symbol"] == "BTCUSDT"
    assert data["direction"] == "BUY"
    assert data["quantity"] == 0.5
    assert data["leverage"] == 5.0
    assert data["hypothesis_id"] == "hypo-999"
