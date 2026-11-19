"""Unit tests for Stage 7 Execution Engine & Hybrid Emergency Rollback Protocol."""

import pytest
from src.core.execution_engine import ExecutionEngine, ExecutionState, EmergencyLockState
from src.core.models import ValidatedOrder


def _make_validated_order(side="BUY", quantity=0.5, price=50000.0):
    """Obtain a genuine HMAC-signed token from RiskEngine (the only issuer)."""
    from src.core.models import TradeHypothesis
    from src.core.risk_engine import RiskEngine

    result = RiskEngine().validate_trade(
        TradeHypothesis(
            symbol="BTCUSDT", direction=side, quantity=quantity, leverage=10.0,
            price=price, stop_loss=49000.0, take_profit=51000.0,
        )
    )
    return result["validated_order"]


def test_execution_engine_initial_state():
    engine = ExecutionEngine()
    assert engine.state == ExecutionState.READY
    assert engine.emergency_lock == EmergencyLockState.UNLOCKED
    assert engine.new_entry_blocked is False


def test_toobit_order_execution_reduce_only():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Execute reduce-only order with ValidatedOrder token
    token = _make_validated_order(side="SELL")
    result = engine.execute_reduce_only_order("BTCUSDT", "SELL", 0.5, 50000.0, validated_order=token)

    assert result["status"] == "SUCCESS"
    assert result["reduce_only"] is True
    assert result["order_type"] == "LIMIT"


def test_toobit_order_execution_ioc():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Execute IOC order with ValidatedOrder token
    token = _make_validated_order(side="SELL")
    result = engine.execute_ioc_order("BTCUSDT", "SELL", 0.5, 50000.0, validated_order=token)

    assert result["status"] == "SUCCESS"
    assert result["time_in_force"] == "IOC"
    assert result["order_type"] == "LIMIT"


def test_toobit_order_execution_market():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Execute market order with ValidatedOrder token
    token = _make_validated_order(side="SELL")
    result = engine.execute_market_order("BTCUSDT", "SELL", 0.5, validated_order=token)

    assert result["status"] == "SUCCESS"
    assert result["order_type"] == "MARKET"


def test_sl_tp_registration_success():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Register SL/TP on fill with ValidatedOrder token
    token = _make_validated_order()
    result = engine.register_sl_tp("BTCUSDT", "BUY", 0.5, 50000.0, 49000.0, 51000.0, validated_order=token)

    assert result["status"] == "SUCCESS"
    assert result["sl_registered"] is True
    assert result["tp_registered"] is True


def test_sl_tp_registration_failure_triggers_rollback():
    engine = ExecutionEngine()

    # Mock Toobit client that fails SL/TP registration
    engine.toobit_client = MockToobitClient(sl_tp_fail=True)

    # Register SL/TP on fill - should trigger rollback
    token = _make_validated_order()
    result = engine.register_sl_tp("BTCUSDT", "BUY", 0.5, 50000.0, 49000.0, 51000.0, validated_order=token)

    assert result["status"] == "ROLLBACK_TRIGGERED"
    assert result["rollback_initiated"] is True


def test_hybrid_emergency_rollback_entry_freeze():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient(sl_tp_fail=True)

    # Trigger rollback
    engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 50000.0)

    # New entry should be blocked
    assert engine.new_entry_blocked is True

    # Try to create new order - should be rejected
    result = engine.create_new_entry_order("BTCUSDT", "BUY", 0.1, 50000.0)
    assert result["status"] == "REJECTED"
    assert "new entry freeze" in result["reason"]


def test_hybrid_emergency_rollback_retry_sl_tp():
    engine = ExecutionEngine()

    # Mock Toobit client that fails first 2 retries but succeeds on 3rd
    engine.toobit_client = MockToobitClient(sl_tp_fail_count=2)

    # Trigger rollback with retries
    result = engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 50000.0)

    # Should retry and eventually succeed
    assert result["status"] == "ROLLBACK_COMPLETED"
    assert result["sl_tp_retries_attempted"] == 3
    assert result["sl_tp_registered"] is True


def test_hybrid_emergency_rollback_ioc_exit():
    engine = ExecutionEngine()

    # Mock Toobit client that fails all retries
    engine.toobit_client = MockToobitClient(sl_tp_fail=True, ioc_fill=True)

    # Trigger rollback
    result = engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 50000.0)

    # Should fall back to IOC exit
    assert result["status"] == "ROLLBACK_COMPLETED"
    assert result["ioc_exit_executed"] is True
    assert result["ioc_fill_status"] == "FILLED"


def test_hybrid_emergency_rollback_market_close_fallback():
    engine = ExecutionEngine()

    # Mock Toobit client that fails retries and IOC doesn't fill
    engine.toobit_client = MockToobitClient(sl_tp_fail=True, ioc_fill=False)

    # Trigger rollback
    result = engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 50000.0)

    # Should fall back to market close
    assert result["status"] == "ROLLBACK_COMPLETED"
    assert result["market_close_executed"] is True


def test_extreme_conditions_bypass():
    engine = ExecutionEngine()

    # Mock extreme conditions
    engine.toobit_client = MockToobitClient(extreme_conditions=True)

    # Trigger rollback in extreme conditions
    result = engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 50000.0)

    # Should bypass retries/IOC and go straight to market liquidation
    assert result["status"] == "ROLLBACK_COMPLETED"
    assert result["extreme_conditions_bypass"] is True
    assert result["market_liquidation_executed"] is True


def test_position_reconciliation():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Reconcile position
    result = engine.reconcile_position("BTCUSDT")

    assert result["status"] == "SUCCESS"
    assert "position_size" in result
    assert result["position_size"] == 0.0  # No position after rollback


def test_emergency_lock_state():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Enter emergency lock
    engine.enter_emergency_lock()

    assert engine.emergency_lock == EmergencyLockState.LOCKED
    assert engine.state == ExecutionState.EMERGENCY_LOCKED

    # Try to execute order - should be rejected
    token = _make_validated_order(side="SELL")
    result = engine.execute_market_order("BTCUSDT", "SELL", 0.5, validated_order=token)
    assert result["status"] == "REJECTED"
    assert "emergency lock" in result["reason"]


def test_manual_operator_reset():
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient()

    # Enter emergency lock
    engine.enter_emergency_lock()

    # Verify position is 0
    assert engine.reconcile_position("BTCUSDT")["position_size"] == 0.0

    # Manual reset
    result = engine.manual_operator_reset()

    assert result["status"] == "SUCCESS"
    assert engine.emergency_lock == EmergencyLockState.UNLOCKED
    assert engine.state == ExecutionState.READY
    assert engine.new_entry_blocked is False


def test_integration_sl_tp_failure_emergency_liquidation():
    """Integration test simulating SL/TP API failure and verifying safe emergency liquidation."""
    engine = ExecutionEngine()

    # Mock Toobit client
    engine.toobit_client = MockToobitClient(sl_tp_fail=True, ioc_fill=False)

    # Simulate order fill
    order_id = engine.create_order_on_fill("BTCUSDT", "BUY", 0.5, 50000.0)

    # Trigger SL/TP registration which should fail
    token = _make_validated_order()
    sl_tp_result = engine.register_sl_tp("BTCUSDT", "BUY", 0.5, 50000.0, 49000.0, 51000.0, validated_order=token)

    # Should trigger rollback
    assert sl_tp_result["status"] == "ROLLBACK_TRIGGERED"

    # Verify final state
    assert engine.emergency_lock == EmergencyLockState.LOCKED
    assert engine.state == ExecutionState.EMERGENCY_LOCKED
    assert engine.reconcile_position("BTCUSDT")["position_size"] == 0.0


# Mock Toobit Client for testing
class MockToobitClient:
    is_simulated = True  # mock: allowed while live trading is disabled

    def __init__(self, sl_tp_fail=False, sl_tp_fail_count=0, ioc_fill=True, extreme_conditions=False):
        self.sl_tp_fail = sl_tp_fail
        self.sl_tp_fail_count = sl_tp_fail_count
        self.sl_tp_attempts = 0
        self.ioc_fill = ioc_fill
        self.extreme_conditions = extreme_conditions
        self.position_size = 0.0

    def register_sl_tp(self, symbol, side, quantity, price, stop_loss, take_profit):
        self.sl_tp_attempts += 1
        if self.sl_tp_fail or (self.sl_tp_fail_count > 0 and self.sl_tp_attempts <= self.sl_tp_fail_count):
            return {"status": "FAILED", "error": "SL/TP registration failed"}
        return {"status": "SUCCESS", "sl_registered": True, "tp_registered": True}

    def execute_reduce_only_order(self, symbol, side, quantity, price, order_type="LIMIT", client_order_id=None):
        return {"status": "SUCCESS", "reduce_only": True, "order_type": order_type}

    def execute_ioc_order(self, symbol, side, quantity, price, client_order_id=None):
        if self.ioc_fill:
            self.position_size = 0.0
            return {"status": "SUCCESS", "time_in_force": "IOC", "order_type": "LIMIT", "fill_status": "FILLED"}
        return {"status": "SUCCESS", "time_in_force": "IOC", "order_type": "LIMIT", "fill_status": "UNFILLED"}

    def execute_market_order(self, symbol, side, quantity, client_order_id=None):
        self.position_size = 0.0
        return {"status": "SUCCESS", "order_type": "MARKET"}

    def get_position(self, symbol):
        return {"position_size": self.position_size}

    def is_extreme_conditions(self):
        return self.extreme_conditions
