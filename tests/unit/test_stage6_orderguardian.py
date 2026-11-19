"""Unit tests for Stage 6 OrderGuardian state machine and race condition defense."""

import pytest
from src.core.order_guardian import OrderGuardian, OrderState, OrderEvent
from src.core.models import ValidatedOrder


def _make_validated_order(side="BUY", quantity=1.0, price=50000.0):
    """Obtain a genuine HMAC-signed token from RiskEngine (the only issuer)."""
    from src.core.models import TradeHypothesis
    from src.core.risk_engine import RiskEngine

    result = RiskEngine().validate_trade(
        TradeHypothesis(
            symbol="BTCUSDT", direction=side, quantity=quantity, leverage=10.0,
            price=price, stop_loss=None, take_profit=None,
        )
    )
    return result["validated_order"]


def test_order_guardian_initial_state():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)

    assert order["state"] == OrderState.CREATED.value
    assert order["symbol"] == "BTCUSDT"
    assert order["side"] == "BUY"
    assert order["quantity"] == 1.0
    assert order["price"] == 50000.0


def test_order_guardian_wires_execution_engine_and_alert_sink():
    from src.core.execution_engine import ExecutionEngine
    from src.core.alerts import AlertSink

    engine = ExecutionEngine()
    sink = AlertSink()
    guardian = OrderGuardian(execution_engine=engine, alert_sink=sink)
    assert guardian.execution_engine is engine
    assert guardian.alert_sink is sink


def test_order_state_transitions():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    # CREATED -> SUBMITTED
    guardian.submit_order(order_id)
    assert guardian.get_order(order_id)["state"] == OrderState.SUBMITTED.value

    # SUBMITTED -> OPEN
    guardian.open_order(order_id)
    assert guardian.get_order(order_id)["state"] == OrderState.OPEN.value


def test_cancel_order_transitions_to_pending_cancel():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)

    # Cancel request should transition to PENDING_CANCEL
    guardian.cancel_order(order_id)
    assert guardian.get_order(order_id)["state"] == OrderState.PENDING_CANCEL.value


def test_cancel_confirmed_transitions_to_cancelled():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)
    guardian.cancel_order(order_id)

    # Cancel confirmed should transition to CANCELLED
    guardian.confirm_cancel(order_id)
    assert guardian.get_order(order_id)["state"] == OrderState.CANCELLED.value


def test_race_condition_fill_overrides_pending_cancel():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)
    guardian.cancel_order(order_id)

    # Fill arrives while in PENDING_CANCEL - should transition to FILLED
    guardian.handle_fill(order_id, filled_quantity=1.0, fill_price=50000.0)

    assert guardian.get_order(order_id)["state"] == OrderState.FILLED.value
    assert guardian.get_order(order_id)["race_condition_resolved"] is True
    assert "RACE_CONDITION_FILL_RESOLVED" in guardian.get_order(order_id)["events"]


def test_race_condition_partial_fill_overrides_pending_cancel():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)
    guardian.cancel_order(order_id)

    # Partial fill arrives while in PENDING_CANCEL
    guardian.handle_fill(order_id, filled_quantity=0.5, fill_price=50000.0)

    assert guardian.get_order(order_id)["state"] == OrderState.PARTIALLY_FILLED.value
    assert guardian.get_order(order_id)["race_condition_resolved"] is True


def test_fill_takes_precedence_over_cancel():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)
    guardian.cancel_order(order_id)

    # Try to cancel again (should be ignored since fill takes precedence)
    guardian.cancel_order(order_id)

    # Now fill arrives
    guardian.handle_fill(order_id, filled_quantity=1.0, fill_price=50000.0)

    assert guardian.get_order(order_id)["state"] == OrderState.FILLED.value
    assert guardian.get_order(order_id)["race_condition_resolved"] is True


def test_volatility_circuit_breaker():
    guardian = OrderGuardian()

    # Simulate high volatility
    guardian.update_volatility("BTCUSDT", 0.05)  # 5% volatility

    # Create order in high volatility
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    # Should not allow new orders in high volatility
    result = guardian.submit_order(order_id)
    assert result["status"] == "REJECTED"
    assert "Volatility circuit breaker" in result["reason"]


def test_emergency_closing_state():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)

    # Enter emergency closing
    guardian.enter_emergency_closing(order_id)
    assert guardian.get_order(order_id)["state"] == OrderState.EMERGENCY_CLOSING.value


def test_rejected_state():
    guardian = OrderGuardian()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    # Reject order
    guardian.reject_order(order_id, "Insufficient margin")
    assert guardian.get_order(order_id)["state"] == OrderState.REJECTED.value
    assert "Insufficient margin" in guardian.get_order(order_id)["rejection_reason"]


def test_position_processing_on_fill():
    from src.core.execution_engine import ExecutionEngine
    from src.core.alerts import AlertSink

    engine = ExecutionEngine()
    engine.toobit_client = MockToobitClient()
    guardian = OrderGuardian(execution_engine=engine, alert_sink=AlertSink())
    token = _make_validated_order()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0, validated_order=token)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)

    # Fill should trigger position processing AND real SL/TP registration
    result = guardian.handle_fill(order_id, filled_quantity=1.0, fill_price=50000.0)

    assert result["position_processed"] is True
    assert result["sl_tp_registered"] is True


def test_handle_fill_without_engine_warns_and_does_not_silently_die():
    """No ExecutionEngine wired -> handle_fill must still return, not crash."""
    guardian = OrderGuardian()  # no execution_engine
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)

    result = guardian.handle_fill(order_id, filled_quantity=1.0, fill_price=50000.0)

    assert result["position_processed"] is True
    assert result["sl_tp_registered"] is False
    assert any(a["level"] == "WARN" for a in guardian.alert_sink.alerts)


def test_handle_fill_emergency_closing_on_sl_tp_failure():
    """When ExecutionEngine.register_sl_tp fails, order must move to EMERGENCY_CLOSING."""
    from src.core.execution_engine import ExecutionEngine
    from src.core.alerts import AlertSink

    engine = ExecutionEngine()
    engine.toobit_client = MockToobitClient(sl_tp_fail=True)
    guardian = OrderGuardian(execution_engine=engine, alert_sink=AlertSink())
    token = _make_validated_order()
    order = guardian.create_order("BTCUSDT", "BUY", 1.0, 50000.0, validated_order=token)
    order_id = order["order_id"]

    guardian.submit_order(order_id)
    guardian.open_order(order_id)

    result = guardian.handle_fill(order_id, filled_quantity=1.0, fill_price=50000.0)

    assert guardian.get_order(order_id)["state"] == OrderState.EMERGENCY_CLOSING.value
    assert any(a["level"] == "ERROR" for a in guardian.alert_sink.alerts)


# Mock Toobit Client for testing
class MockToobitClient:
    is_simulated = True  # mock: allowed while live trading is disabled

    def __init__(self, sl_tp_fail=False):
        self.sl_tp_fail = sl_tp_fail

    def register_sl_tp(self, symbol, side, quantity, price, stop_loss, take_profit):
        if self.sl_tp_fail:
            return {"status": "FAILED", "error": "SL/TP registration failed"}
        return {"status": "SUCCESS", "sl_registered": True, "tp_registered": True}

    def execute_reduce_only_order(self, symbol, side, quantity, price, order_type="LIMIT", client_order_id=None):
        return {"status": "SUCCESS", "reduce_only": True, "order_type": order_type}

    def execute_ioc_order(self, symbol, side, quantity, price, client_order_id=None):
        return {"status": "SUCCESS", "time_in_force": "IOC", "order_type": "LIMIT", "fill_status": "FILLED"}

    def execute_market_order(self, symbol, side, quantity, client_order_id=None):
        return {"status": "SUCCESS", "order_type": "MARKET"}

    def get_position(self, symbol):
        return {"position_size": 0.0}

    def is_extreme_conditions(self):
        return False
