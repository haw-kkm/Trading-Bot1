"""Track A 2nd pass: fill-state classification, whole-account reset, position-based rollback.

Real PostgreSQL for the order store; PaperExchange (assumed contract, NOT Toobit) for the exchange.
"""

import pytest

from src.core.execution_engine import (
    EmergencyLockState, ExecutionEngine, FillState, classify_fill,
)
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine
from src.orders.pg_store import IntentState
from src.orders.submitter import SafeSubmitter
from tests.support.paper_exchange import PaperExchange

CID = "tbabc"


def _engine(store, ex):
    e = ExecutionEngine(order_store=store)
    e.toobit_client = ex
    return e


def _token(side="BUY", qty=0.1):
    return RiskEngine().validate_trade(TradeHypothesis(
        symbol="BTCUSDT", direction=side, quantity=qty, leverage=10.0, price=50000.0,
        stop_loss=49000.0, take_profit=51000.0))["validated_order"]


def _kinds(ex):
    return [k for k, *_ in ex.sent]


# ===================================================================== 1. fill classification
def _resp(status, executed, **kw):
    r = {"clientOrderId": CID, "status": status, "executedQty": executed}
    r.update(kw)
    return r


@pytest.mark.parametrize("resp,state", [
    (_resp("FILLED", 0.5), FillState.FILLED),
    (_resp("EXPIRED", 0.2), FillState.PARTIAL),                 # terminal + partial fill exists
    (_resp("REJECTED", 0.2), FillState.PARTIAL),                # REJECTED is NOT "zero filled"
    (_resp("CANCELED", 0.2), FillState.PARTIAL),
    (_resp("EXPIRED", 0.0), FillState.UNFILLED_CONFIRMED),      # terminal AND zero executed
    (_resp("REJECTED", 0.0), FillState.UNFILLED_CONFIRMED),
    (_resp("NEW", 0.0), FillState.UNCLEAR),                     # still working
    (_resp("PARTIALLY_FILLED", 0.2), FillState.UNCLEAR),        # non-terminal partial may change
    (_resp("EXPIRED", None), FillState.UNCLEAR),                # terminal but qty unknown: no inference
    (_resp("REJECTED", "abc"), FillState.UNCLEAR),
    (_resp("REJECTED", -1), FillState.UNCLEAR),
    (_resp("FILLED", 0.2), FillState.UNCLEAR),                  # status/qty conflict
    (_resp("EXPIRED", 0.5), FillState.UNCLEAR),                 # full qty but not FILLED: conflict
    (_resp("FILLED", 0.9), FillState.UNCLEAR),                  # overfill
    (_resp("", 0.5), FillState.UNCLEAR),
    ({"status": "FILLED", "executedQty": 0.5}, FillState.UNCLEAR),   # no clientOrderId
    (_resp("FILLED", 0.5, clientOrderId="other"), FillState.UNCLEAR),
    ({}, FillState.UNCLEAR), (None, FillState.UNCLEAR), ("garbage", FillState.UNCLEAR),
])
def test_classify_fill(resp, state):
    a = classify_fill(resp, CID, 0.5)
    assert a.state == state, a


def test_missing_fill_status_never_defaults_to_filled_or_unfilled():
    # a bare ACK / mock `fill_status` field is not consulted at all
    assert classify_fill({"status": "SUCCESS", "orderId": "1", "clientOrderId": CID}, CID, 0.5).state \
        == FillState.UNCLEAR
    assert classify_fill({"clientOrderId": CID, "status": "NEW", "fill_status": "FILLED"}, CID, 0.5).state \
        == FillState.UNCLEAR


def test_fill_status_missing_blocks_additional_market_order(store):
    ex = PaperExchange(0.5)
    ex.ioc_fill_ratio = 0.0            # position still open after the IOC ACK
    ex.query_mode = "no_qty"           # query exists but carries no cumulative quantity
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED"
    assert _kinds(ex) == ["ioc"]       # NO market order
    assert e.emergency_lock == EmergencyLockState.LOCKED and e.new_entry_blocked
    assert r["stages"]["last_fill_assessment"]["state"] == "UNCLEAR"


def test_ack_success_but_fill_unconfirmed_sends_nothing_more(store):
    ex = PaperExchange(0.5)
    ex.ioc_fill_ratio = 0.0
    ex.query_mode = "empty"            # order query returns nothing
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and _kinds(ex) == ["ioc"]


def test_ioc_partial_fill_closes_only_actual_remainder(store):
    ex = PaperExchange(0.5)
    ex.ioc_fill_ratio = 0.4            # 0.2 filled, 0.3 remains
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_COMPLETED" and r["market_close_executed"] is True
    assert [(k, q) for k, _s, q, *_ in ex.sent] == [("ioc", 0.5), ("market", pytest.approx(0.3))]
    assert all(x[4] is True for x in ex.sent)            # reduce-only on every close order
    assert ex.positions["BTCUSDT"] == pytest.approx(0.0)


def test_terminal_rejected_with_partial_fill_still_uses_cumulative_quantity(store):
    ex = PaperExchange(0.5)
    ex.ioc_fill_ratio = 0.4
    ex.query_status_override = "REJECTED"   # terminal status, but 0.2 DID fill
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_COMPLETED"
    # market (if any) must be for the 0.3 remainder, never the full 0.5 and never zero-fill assumed
    mk = [q for k, _s, q, *_ in ex.sent if k == "market"]
    assert mk == [pytest.approx(0.3)]


def test_market_ack_but_actually_unfilled_sends_no_further_order(store):
    ex = PaperExchange(0.5, extreme=True)
    ex.market_fill_ratio = 0.0
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and r["remaining"] == pytest.approx(0.5)
    assert _kinds(ex) == ["market"] and e.emergency_lock == EmergencyLockState.LOCKED
    assert r["stages"]["close_confirmed"] is False


def test_order_query_timeout_blocks_market_fallback(store):
    ex = PaperExchange(0.5)
    ex.ioc_fill_ratio = 0.0
    ex.query_mode = "raise"
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and _kinds(ex) == ["ioc"]
    assert "query failed" in r["stages"]["last_fill_assessment"]["reason"]


def test_order_state_conflicting_with_position_blocks_market_fallback(store):
    ex = PaperExchange(0.5)
    ex.ioc_fill_ratio = 0.0            # position really unchanged ...
    ex.query_status_override = "FILLED"
    ex.query_executed_override = 0.5   # ... but the order claims a full fill
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and _kinds(ex) == ["ioc"]
    assert "position mismatch" in r["stages"]["last_fill_assessment"]["reason"]


# ===================================================================== 4. position-based rollback
@pytest.mark.parametrize("pos,close_side", [(0.5, "SELL"), (-0.5, "BUY")])
def test_close_direction_follows_actual_position_long_and_short(store, pos, close_side):
    ex = PaperExchange(pos)
    r = _engine(store, ex).trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)   # caller side is irrelevant
    assert r["status"] == "ROLLBACK_COMPLETED"
    assert ex.sent[0][1] == close_side and ex.sent[0][4] is True


def test_requested_quantity_mismatch_uses_actual_position(store):
    ex = PaperExchange(0.3)
    r = _engine(store, ex).trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_COMPLETED" and ex.sent[0][2] == 0.3


@pytest.mark.parametrize("mode", ["HEDGE", None, "WEIRD", "raise"])
def test_unclear_or_hedge_position_mode_blocks_all_orders(store, mode):
    ex = PaperExchange(0.5, mode=mode)
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_BLOCKED_POSITION_UNKNOWN" and ex.sent == []   # no fallback to caller values
    assert e.emergency_lock == EmergencyLockState.LOCKED and e.new_entry_blocked
    assert any(a["level"] == "ERROR" and "OPERATOR ACTION" in a["message"] for a in e.alert_sink.alerts)


@pytest.mark.parametrize("mode", ["raise", "incomplete", "stale", "malformed"])
def test_position_query_failure_blocks_orders_and_freezes(store, mode):
    ex = PaperExchange(0.5)
    ex.positions_mode = mode
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_BLOCKED_POSITION_UNKNOWN" and ex.sent == []
    assert e.emergency_lock == EmergencyLockState.LOCKED and store.entries_frozen()


def test_adapter_without_account_api_blocks_rollback_orders(store):
    class Legacy:
        is_simulated = True

        def is_extreme_conditions(self):
            return False

        def register_sl_tp(self, *a):
            return {"status": "FAILED"}
    e = _engine(store, Legacy())
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_BLOCKED_POSITION_UNKNOWN" and r["kind"] == "NOT_IMPLEMENTED"


def _entry_order(oid="O1", qty=1.0):
    return {"orderId": oid, "symbol": "BTCUSDT", "side": "BUY", "reduceOnly": False, "qty": qty}


def test_flat_position_but_open_entry_order_is_cancelled_and_confirmed(store):
    ex = PaperExchange(0.0)
    ex.open_orders.append(_entry_order())
    r = _engine(store, ex).trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_COMPLETED" and r["stages"]["entry_cancel_requested"] is True
    assert r["stages"]["entry_orders_cleared_confirmed"] is True and ex.open_orders == []
    assert ex.sent == []                         # nothing to close, no order sent


def test_cancel_requested_but_not_confirmed_keeps_lock(store):
    ex = PaperExchange(0.0)
    ex.open_orders.append(_entry_order())
    ex.cancel_confirms = False                   # cancel REQUEST accepted, order still live
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and "not confirmed cancelled" in r["reason"]
    assert r["stages"]["entry_cancel_requested"] and not r["stages"]["entry_orders_cleared_confirmed"]
    assert e.emergency_lock == EmergencyLockState.LOCKED


def test_cancel_vs_fill_race_reopens_position_and_keeps_lock(store):
    ex = PaperExchange(0.0)
    ex.open_orders.append(_entry_order(qty=1.0))
    ex.fill_open_order_on_cancel = True          # fill wins the race against the cancel
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and "position" in r["reason"]
    assert ex.positions["BTCUSDT"] == 1.0 and ex.sent == []      # no automatic second liquidation
    assert e.emergency_lock == EmergencyLockState.LOCKED


def test_flat_and_orders_closed_but_protective_orders_not_cleared_keeps_lock(store):
    ex = PaperExchange(0.5)
    ex.conditional_orders.append({"orderId": "SL1", "symbol": "BTCUSDT"})
    ex.cancel_conditional_confirms = False
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    s = r["stages"]
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and "SL/TP" in r["reason"]
    assert s["close_confirmed"] and s["entry_orders_cleared_confirmed"]
    assert s["protective_cancel_requested"] and not s["protective_orders_cleared_confirmed"]
    assert e.emergency_lock == EmergencyLockState.LOCKED


def test_protective_orders_cleared_and_confirmed_completes(store):
    ex = PaperExchange(0.5)
    ex.conditional_orders.append({"orderId": "SL1", "symbol": "BTCUSDT"})
    r = _engine(store, ex).trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_COMPLETED" and r["stages"]["protective_orders_cleared_confirmed"]


def test_post_close_verification_query_failure_is_not_completion(store):
    ex = PaperExchange(0.5)
    ex.open_orders_mode = "incomplete"
    r = _engine(store, ex).trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and "verification incomplete" in r["reason"]


# ===================================================================== 2. whole-account reset
def _locked(store, ex):
    e = _engine(store, ex)
    e.enter_emergency_lock()
    return e


def test_reset_succeeds_only_on_fully_verified_flat_account(store):
    ex = PaperExchange(0.0)
    e = _locked(store, ex)
    r = e.manual_operator_reset()
    assert r["status"] == "SUCCESS" and "account_drawdown_limit" in r["unverified"]
    assert e.new_entry_blocked is False and not store.entries_frozen()


def test_reset_refuses_when_other_symbol_has_exposure(store):
    ex = PaperExchange(0.0)                      # BTC flat ...
    ex.positions["ETHUSDT"] = 1.0                # ... but ETH is not
    e = _locked(store, ex)
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "EXPOSURE_PRESENT" and r["symbols"] == ["ETHUSDT"]
    assert r["verification_incomplete"] is False
    assert e.emergency_lock == EmergencyLockState.LOCKED and store.entries_frozen()


def test_reset_refuses_with_open_orders_even_if_flat(store):
    ex = PaperExchange(0.0)
    ex.open_orders.append({"orderId": "O9", "symbol": "SOLUSDT", "side": "BUY", "reduceOnly": False})
    r = _locked(store, ex).manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "OPEN_ORDERS_PRESENT"


def test_reset_refuses_with_conditional_orders_and_does_not_auto_cancel(store):
    ex = PaperExchange(0.0)
    ex.conditional_orders.append({"orderId": "SL9", "symbol": "SOLUSDT"})
    r = _locked(store, ex).manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "CONDITIONAL_ORDERS_PRESENT"
    assert len(ex.conditional_orders) == 1


@pytest.mark.parametrize("attr,mode", [
    ("positions_mode", "raise"), ("positions_mode", "incomplete"), ("positions_mode", "stale"),
    ("positions_mode", "malformed"), ("open_orders_mode", "raise"), ("open_orders_mode", "incomplete"),
    ("conditional_mode", "raise"), ("conditional_mode", "stale"),
])
def test_reset_refuses_on_incomplete_or_stale_queries_and_says_so(store, attr, mode):
    ex = PaperExchange(0.0)
    setattr(ex, attr, mode)
    e = _locked(store, ex)
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "INCOMPLETE_VERIFICATION"
    assert r["verification_incomplete"] is True and "does NOT mean a position exists" in r["reason"]
    assert e.emergency_lock == EmergencyLockState.LOCKED and store.entries_frozen()


def test_reset_refuses_when_adapter_has_no_account_queries(store):
    class Legacy:
        is_simulated = True

        def get_position(self, symbol):
            return {"position_size": 0.0}
    e = _locked(store, Legacy())
    r = e.manual_operator_reset()
    assert r["kind"] == "NOT_IMPLEMENTED" and r["verification_incomplete"] is True
    assert e.emergency_lock == EmergencyLockState.LOCKED


def test_reset_refuses_with_unresolved_intent_in_db(store):
    ex = PaperExchange(0.0)
    e = _engine(store, ex)
    ex2 = PaperExchange(0.0)
    ex2.fail_sends = TimeoutError("lost")
    e2 = _engine(store, ex2)
    assert e2.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())["status"] == "SUBMISSION_UNKNOWN"
    r = e.manual_operator_reset()
    assert r["kind"] == "UNRESOLVED_INTENTS"


# ---- UNKNOWN resolution is necessary but NOT sufficient for resuming entries ---------
def test_resolving_unknown_does_not_resume_entries_only_reset_does(store):
    ex = PaperExchange(0.0)
    ex.fail_sends = TimeoutError("lost")
    e = _engine(store, ex)
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())["status"] == "SUBMISSION_UNKNOWN"
    unk = store.list_by_state(IntentState.SUBMISSION_UNKNOWN)[0]
    # evidence from the exchange resolves the UNKNOWN as ACCEPTED
    sub = SafeSubmitter(store, lambda i: None, query=lambda c: {
        "clientOrderId": unk.client_order_id, "orderId": "77", "status": "NEW",
        "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.1"})
    assert sub.reconcile(unk.intent_id).state == IntentState.ACCEPTED
    assert not store.has_unresolved_intents() or store.list_by_state(IntentState.ACCEPTED)
    # resolution alone changed neither the in-memory flag nor the persisted freeze
    assert e.new_entry_blocked is True and store.entries_frozen()
    ex.fail_sends = None
    out = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())
    assert out["status"] == "REJECTED" and ex.sent and len(ex.sent) == 1   # blocked; nothing new sent
    # a brand-new engine (other process / restart) is also still blocked: the freeze is persisted
    e_other = _engine(store, PaperExchange(0.0))
    assert e_other.new_entry_blocked is True
    # open exposure keeps the reset refused even though UNKNOWN is resolved
    ex.positions["BTCUSDT"] = 0.1
    assert e.manual_operator_reset()["kind"] == "EXPOSURE_PRESENT"
    ex.positions["BTCUSDT"] = 0.0
    assert e.manual_operator_reset()["status"] == "SUCCESS"                 # only now entries may resume
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())["status"] == "SUCCESS"
