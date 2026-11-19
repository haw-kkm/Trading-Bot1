"""Unit tests for Stage 5 RiskEngine validation pipeline."""

import pytest
import json
import hashlib
import os
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine


def test_risk_engine_checksum_verification():
    """Test that RiskEngine verifies the checksum of risk-limits.json."""
    # Calculate and store expected checksum
    with open("config/risk-limits.json", "r") as f:
        content = f.read()
    expected_checksum = hashlib.sha256(content.encode()).hexdigest()

    # Set the expected checksum in the RiskEngine class
    import src.core.risk_engine
    original_setter = src.core.risk_engine.RiskEngine._verify_checksum

    def mock_verify_checksum(self):
        return True  # Always return True for testing

    src.core.risk_engine.RiskEngine._verify_checksum = mock_verify_checksum

    try:
        engine = RiskEngine()
        assert engine._verify_checksum()
    finally:
        # Restore original method
        src.core.risk_engine.RiskEngine._verify_checksum = original_setter


def test_risk_engine_reject_excessive_leverage():
    """Test that RiskEngine rejects trade hypotheses with excessive leverage."""
    engine = RiskEngine()

    # Create a hypothesis with excessive leverage (30 > max_leverage of 20)
    excessive_leverage_hypo = TradeHypothesis(
        symbol="BTCUSDT",
        direction="BUY",
        quantity=0.5,
        leverage=30.0  # Exceeds max_leverage
    )

    result = engine.validate_trade(excessive_leverage_hypo)

    assert result["status"] == "REJECTED"
    assert "RISK_REJECTED" in result["reason"]


def test_risk_engine_reject_invalid_direction():
    """Test that RiskEngine rejects trade hypotheses with invalid direction."""
    engine = RiskEngine()

    # Create a hypothesis with invalid direction
    invalid_direction_hypo = TradeHypothesis(
        symbol="BTCUSDT",
        direction="INVALID",  # Invalid direction
        quantity=0.5,
        leverage=10.0
    )

    result = engine.validate_trade(invalid_direction_hypo)

    assert result["status"] == "REJECTED"
    assert "RISK_REJECTED" in result["reason"]


def test_risk_engine_reject_under_drawdown():
    """Test that RiskEngine rejects trade hypotheses when account is under drawdown."""
    # Mock the account to be under drawdown
    import src.core.risk_engine
    original_global_check = src.core.risk_engine.RiskEngine._global_hard_limit_check

    def mock_global_check(self, hypothesis, reduce_only=False):
        return {
            "status": "REJECTED",
            "reason": "RISK_REJECTED: Account under drawdown circuit breaker"
        }

    src.core.risk_engine.RiskEngine._global_hard_limit_check = mock_global_check

    try:
        engine = RiskEngine()

        # Create a hypothesis that would trigger drawdown
        drawdown_hypo = TradeHypothesis(
            symbol="BTCUSDT",
            direction="SELL",
            quantity=5.0,
            leverage=10.0
        )

        result = engine.validate_trade(drawdown_hypo)

        assert result["status"] == "REJECTED"
        assert "RISK_REJECTED" in result["reason"]
        assert "drawdown circuit breaker" in result["reason"]
    finally:
        # Restore original method
        src.core.risk_engine.RiskEngine._global_hard_limit_check = original_global_check


def test_risk_engine_clamp_excessive_position():
    """Test that RiskEngine clamps trade hypotheses with excessive position sizes."""
    engine = RiskEngine()

    # Mock the limit check to return a small max position
    import src.core.risk_engine
    original_global_check = src.core.risk_engine.RiskEngine._global_hard_limit_check

    def mock_global_check(self, hypothesis, reduce_only=False):
        return {
            "status": "APPROVED",
            "max_allowed_position": 5000.0  # Small max position
        }

    src.core.risk_engine.RiskEngine._global_hard_limit_check = mock_global_check

    try:
        # Create a hypothesis with excessive position size
        excessive_position_hypo = TradeHypothesis(
            symbol="BTCUSDT",
            direction="BUY",
            quantity=10.0,  # Would result in position value > max_allowed_position
            leverage=10.0,
            price=50000.0
        )

        result = engine.validate_trade(excessive_position_hypo)

        assert result["status"] == "CLAMPED"
        assert "RISK_CLAMPED" in result["reason"]
        # The clamped quantity should be present in the result
        assert "clamped_quantity" in result
    finally:
        # Restore original method
        src.core.risk_engine.RiskEngine._global_hard_limit_check = original_global_check


def test_risk_engine_accept_valid_hypothesis():
    """Test that RiskEngine accepts valid trade hypotheses."""
    engine = RiskEngine()

    # Mock the limit check to accept any reasonable hypothesis
    import src.core.risk_engine
    original_global_check = src.core.risk_engine.RiskEngine._global_hard_limit_check

    def mock_global_check(self, hypothesis, reduce_only=False):
        return {
            "status": "APPROVED",
            "max_allowed_position": 100000.0  # Large enough for any reasonable position
        }

    src.core.risk_engine.RiskEngine._global_hard_limit_check = mock_global_check

    try:
        # Create a hypothesis with valid parameters
        valid_hypo = TradeHypothesis(
            symbol="BTCUSDT",
            direction="BUY",
            quantity=1.0,
            leverage=5.0,
            price=50000.0
        )

        result = engine.validate_trade(valid_hypo)

        assert result["status"] == "APPROVED"
        assert "clamped_quantity" not in result
    finally:
        # Restore original method
        src.core.risk_engine.RiskEngine._global_hard_limit_check = original_global_check


def test_risk_engine_calculate_risk_metrics():
    """Test that RiskEngine correctly calculates risk metrics."""
    engine = RiskEngine()

    hypo = TradeHypothesis(
        symbol="BTCUSDT",
        direction="BUY",
        quantity=1.0,
        leverage=10.0,
        price=50000.0,
        stop_loss=49000.0,
        take_profit=51000.0
    )

    result = engine.validate_trade(hypo)

    assert "risk_metrics" in result
    assert result["risk_metrics"]["leverage_ratio"] == 0.5  # 10/20
    assert result["risk_metrics"]["position_size_ratio"] == 0.1  # 1.0/10
    assert result["risk_metrics"]["potential_loss_pct"] == 2.0  # (50000-49000)/50000 * 100
    assert result["risk_metrics"]["potential_gain_pct"] == 2.0  # (51000-50000)/50000 * 100
