"""Stage 8 safety tests: bypass block, clamp enforcement, real SL/TP, exception safety."""

import dataclasses

import pytest

from src.core.alerts import AlertSink
from src.core.execution_engine import (
    EmergencyLockState,
    ExecutionEngine,
    UnauthorizedExecutionError,
)
from src.core.models import TradeHypothesis, ValidatedOrder
from src.core.order_guardian import OrderGuardian, OrderState
from src.core.risk_engine import RiskEngine


class RecordingClient:
    is_simulated = True  # mock: allowed while live trading is disabled

    """Mock Toobit client that records every call it receives."""

    def __init__(self, sl_tp_ok=True, raise_on=None):
        self.sl_tp_ok = sl_tp_ok
        self.raise_on = raise_on or set()
        self.calls = []

    def _maybe_raise(self, name):
        if name in self.raise_on:
            raise ConnectionError(f"{name} boom")

    def register_sl_tp(self, symbol, side, quantity, price, stop_loss, take_profit):
        self.calls.append(("register_sl_tp", symbol, side, quantity, price, stop_loss, take_profit))
        self._maybe_raise("register_sl_tp")
        return {"status": "SUCCESS" if self.sl_tp_ok else "FAILED"}

    def execute_reduce_only_order(self, symbol, side, quantity, price, order_type="LIMIT", client_order_id=None):
        self.calls.append(("reduce_only", symbol, side, quantity, price))
        self._maybe_raise("execute_reduce_only_order")
        return {"status": "SUCCESS"}

    def execute_ioc_order(self, symbol, side, quantity, price, client_order_id=None):
        self.calls.append(("ioc", symbol, side, quantity, price))
        self._maybe_raise("execute_ioc_order")
        return {"status": "SUCCESS", "fill_status": "FILLED"}

    def execute_market_order(self, symbol, side, quantity, client_order_id=None):
        self.calls.append(("market", symbol, side, quantity))
        self._maybe_raise("execute_market_order")
        return {"status": "SUCCESS"}

    def get_position(self, symbol):
        return {"position_size": 0.0}

    def is_extreme_conditions(self):
        return False


def _hypothesis(quantity=0.1, price=50000.0):
    return TradeHypothesis(
        symbol="BTCUSDT",
        direction="BUY",
        quantity=quantity,
        leverage=10.0,
        price=price,
        stop_loss=49000.0,
        take_profit=51000.0,
        hypothesis_id="h-1",
    )


def _clamped_token():
    result = RiskEngine().validate_trade(_hypothesis(quantity=5.0))
    assert result["status"] == "CLAMPED"
    return result["validated_order"]


# 1. Bypass block ---------------------------------------------------------

@pytest.mark.parametrize(
    "call",
    [
        lambda e: e.execute_reduce_only_order("BTCUSDT", "SELL", 1.0, 50000.0),
        lambda e: e.execute_ioc_order("BTCUSDT", "SELL", 1.0, 50000.0),
        lambda e: e.execute_market_order("BTCUSDT", "SELL", 1.0),
        lambda e: e.register_sl_tp("BTCUSDT", "BUY", 1.0, 50000.0, 49000.0, 51000.0),
    ],
)
def test_execute_without_token_raises_and_never_reaches_client(call):
    engine = ExecutionEngine()
    client = RecordingClient()
    engine.toobit_client = client
    with pytest.raises(UnauthorizedExecutionError):
        call(engine)
    assert client.calls == []


def test_execute_with_forged_token_raises():
    engine = ExecutionEngine()
    client = RecordingClient()
    engine.toobit_client = client
    forged = ValidatedOrder(
        symbol="BTCUSDT", side="BUY", quantity=999.0, leverage=10.0, price=50000.0,
        stop_loss=None, take_profit=None, hypothesis_id=None, status="APPROVED",
        original_quantity=999.0, signature="deadbeef",
    )
    with pytest.raises(UnauthorizedExecutionError):
        engine.execute_market_order("BTCUSDT", "BUY", 999.0, validated_order=forged)
    assert client.calls == []


def test_token_is_immutable_and_tamper_detected():
    token = _clamped_token()
    with pytest.raises(dataclasses.FrozenInstanceError):
        token.quantity = 100.0
    tampered = dataclasses.replace(token, quantity=100.0)  # keeps old signature
    assert RiskEngine.verify_token(token) is True
    assert RiskEngine.verify_token(tampered) is False


def test_rejected_trade_gets_no_token():
    result = RiskEngine().validate_trade(
        TradeHypothesis(symbol="BTCUSDT", direction="BUY", quantity=1.0, leverage=1000.0, price=50000.0)
    )
    assert result["status"] == "REJECTED"
    assert "validated_order" not in result


# 2. Clamp enforcement ----------------------------------------------------

def test_engine_uses_clamped_quantity_not_caller_quantity():
    token = _clamped_token()
    assert token.quantity < token.original_quantity
    engine = ExecutionEngine()
    client = RecordingClient()
    engine.toobit_client = client
    # Caller tries to pass the ORIGINAL (unclamped) quantity.
    engine.execute_market_order("BTCUSDT", "BUY", token.original_quantity, validated_order=token)
    assert client.calls[0] == ("market", "BTCUSDT", "BUY", token.quantity)


def test_guardian_order_uses_clamped_quantity():
    token = _clamped_token()
    guardian = OrderGuardian(alert_sink=AlertSink())
    order = guardian.create_order(
        "BTCUSDT", "BUY", token.original_quantity, 50000.0, validated_order=token
    )
    assert order["quantity"] == token.quantity
    assert guardian.get_order(order["order_id"])["quantity"] == token.quantity


def test_guardian_rejects_tampered_token():
    token = dataclasses.replace(_clamped_token(), quantity=100.0)
    guardian = OrderGuardian(alert_sink=AlertSink())
    with pytest.raises(ValueError):
        guardian.create_order("BTCUSDT", "BUY", 100.0, 50000.0, validated_order=token)
    assert any(a["level"] == "ERROR" for a in guardian.alert_sink.alerts)


# 3. Real SL/TP -----------------------------------------------------------

def test_handle_fill_calls_real_register_sl_tp_with_token_values():
    token = RiskEngine().validate_trade(_hypothesis())["validated_order"]
    engine = ExecutionEngine()
    client = RecordingClient()
    engine.toobit_client = client
    calls = []
    original = engine.register_sl_tp

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    engine.register_sl_tp = spy
    guardian = OrderGuardian(execution_engine=engine, alert_sink=AlertSink())
    oid = guardian.create_order("BTCUSDT", "BUY", 0.1, 50000.0, validated_order=token)["order_id"]
    guardian.submit_order(oid)
    guardian.open_order(oid)
    result = guardian.handle_fill(oid, filled_quantity=0.1, fill_price=50000.0)

    assert len(calls) == 1
    assert calls[0][1]["validated_order"] is token
    assert result["sl_tp_registered"] is True
    sl_call = [c for c in client.calls if c[0] == "register_sl_tp"][0]
    assert sl_call[5] == 49000.0 and sl_call[6] == 51000.0


# 4. Exception safety -----------------------------------------------------

def _filled_guardian(engine, token):
    guardian = OrderGuardian(execution_engine=engine, alert_sink=AlertSink())
    oid = guardian.create_order("BTCUSDT", "BUY", 0.1, 50000.0, validated_order=token)["order_id"]
    guardian.submit_order(oid)
    guardian.open_order(oid)
    return guardian, oid


def test_register_sl_tp_client_exception_triggers_rollback_and_emergency_closing():
    token = RiskEngine().validate_trade(_hypothesis())["validated_order"]
    engine = ExecutionEngine()
    engine.toobit_client = RecordingClient(raise_on={"register_sl_tp"})
    guardian, oid = _filled_guardian(engine, token)

    result = guardian.handle_fill(oid, 0.1, 50000.0)

    assert result["sl_tp_registered"] is False
    assert guardian.get_order(oid)["state"] == OrderState.EMERGENCY_CLOSING.value
    assert engine.rollback_calls >= 1
    assert engine.emergency_lock == EmergencyLockState.LOCKED
    assert any(a["level"] == "ERROR" for a in guardian.alert_sink.alerts)


def test_client_none_does_not_die_silently():
    token = RiskEngine().validate_trade(_hypothesis())["validated_order"]
    engine = ExecutionEngine()
    engine.toobit_client = None
    guardian, oid = _filled_guardian(engine, token)

    result = guardian.handle_fill(oid, 0.1, 50000.0)

    assert result["sl_tp_registered"] is False
    assert guardian.get_order(oid)["state"] == OrderState.EMERGENCY_CLOSING.value
    assert engine.rollback_calls >= 1
    assert engine.new_entry_blocked is True
    assert engine.emergency_lock == EmergencyLockState.LOCKED


def test_execute_client_exception_is_submission_unknown_not_rollback():
    """Req: an API timeout/connection error is NOT a failure. No rollback, no resend,
    new entries frozen, outcome persisted as SUBMISSION_UNKNOWN."""
    token = RiskEngine().validate_trade(_hypothesis())["validated_order"]
    engine = ExecutionEngine()
    client = RecordingClient(raise_on={"execute_market_order", "register_sl_tp"})
    engine.toobit_client = client
    result = engine.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=token)
    assert result["status"] == "SUBMISSION_UNKNOWN"
    assert getattr(engine, "rollback_calls", 0) == 0
    assert engine.new_entry_blocked is True
    assert [c[0] for c in client.calls] == ["market"]  # sent exactly once, nothing else
    assert engine.order_store.has_unresolved_unknown()


def test_rollback_itself_never_raises_when_client_missing():
    engine = ExecutionEngine()
    engine.toobit_client = None
    result = engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.1, 50000.0)
    assert result["status"] == "ROLLBACK_FAILED_LOCKED"
    assert engine.emergency_lock == EmergencyLockState.LOCKED


def test_missing_token_on_fill_moves_to_emergency_closing():
    engine = ExecutionEngine()
    engine.toobit_client = RecordingClient()
    guardian = OrderGuardian(execution_engine=engine, alert_sink=AlertSink())
    oid = guardian.create_order("BTCUSDT", "BUY", 0.1, 50000.0)["order_id"]
    guardian.submit_order(oid)
    guardian.open_order(oid)
    guardian.handle_fill(oid, 0.1, 50000.0)
    assert guardian.get_order(oid)["state"] == OrderState.EMERGENCY_CLOSING.value
    assert engine.rollback_calls >= 1


# 5. HMAC signing & rollback price ----------------------------------------

def test_sha256_forgery_with_known_algorithm_is_rejected():
    """Attacker who knows the canonical form + plain SHA-256 still can't forge."""
    import hashlib

    fields = dict(
        symbol="BTCUSDT", side="BUY", quantity=999.0, leverage=10.0, price=50000.0,
        stop_loss=None, take_profit=None, hypothesis_id=None, status="APPROVED",
        original_quantity=999.0,
    )
    sig = hashlib.sha256(ValidatedOrder.canonical_payload(**fields)).hexdigest()
    forged = ValidatedOrder(**fields, signature=sig)
    assert RiskEngine.verify_token(forged) is False
    engine = ExecutionEngine()
    engine.toobit_client = RecordingClient()
    with pytest.raises(UnauthorizedExecutionError):
        engine.execute_market_order("BTCUSDT", "BUY", 999.0, validated_order=forged)


def test_signature_is_hmac_bound_to_secret_key(monkeypatch):
    token = RiskEngine().validate_trade(_hypothesis())["validated_order"]
    assert RiskEngine.verify_token(token) is True
    monkeypatch.setattr(RiskEngine, "_SIGNING_KEY", b"x" * 32)
    assert RiskEngine.verify_token(token) is False


def test_emergency_rollback_uses_caller_price_via_persisted_intent():
    engine = ExecutionEngine()
    engine.toobit_client = RecordingClient(sl_tp_ok=False)
    engine.trigger_emergency_rollback("BTCUSDT", "BUY", 0.1, 42000.0)
    ioc = [c for c in engine.toobit_client.calls if c[0] == "ioc"]
    assert ioc and ioc[0][4] == 42000.0