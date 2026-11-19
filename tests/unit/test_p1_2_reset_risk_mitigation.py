"""P1-2: manual_operator_reset is a RISK MITIGATION, not an atomicity guarantee.

The exchange query and the DB freeze release cannot be one atomic step. These tests cover the
mitigations (in-flight refusal, stale/error refusal, double pre-release verification, post-release
re-verification with re-freeze). The residual race is documented, NOT claimed solved
(see docs/plans/track-a-safety-guarantees.md). Exchange responses are an ASSUMED contract."""
import pytest

from src.core.execution_engine import EmergencyLockState, ExecutionEngine
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine
from src.orders.errors import EntriesBlocked
from src.orders.pg_store import OrderStore
from tests.support.paper_exchange import PaperExchange


def _engine(store, ex):
    e = ExecutionEngine(order_store=store)
    e.toobit_client = ex
    return e


def _locked(store, ex):
    e = _engine(store, ex)
    e.enter_emergency_lock()
    return e


def _tok(side, reduce_only):
    return RiskEngine().validate_trade(TradeHypothesis(
        symbol="BTCUSDT", direction=side, quantity=0.1, leverage=5.0, price=50000.0,
        stop_loss=None, take_profit=None), reduce_only=reduce_only)["validated_order"]


def _still_locked(e, store):
    assert e.emergency_lock == EmergencyLockState.LOCKED
    assert store.entries_frozen()


def _entry_auth(store, aid):
    store.issue_authorization(aid, "ENTRY", "BTCUSDT", "BUY", "MARKET", 0.1, None, False)


# ------------------------------------------------------------ in-flight orders block release
def test_in_flight_submit_pending_intent_blocks_reset(store):
    ex = PaperExchange(0.0)
    _entry_auth(store, "a1")
    store.consume_persist_and_claim("a1")            # SUBMIT_PENDING: request may be on the wire
    e = _locked(store, ex)
    store.freeze_entries("test")
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "UNRESOLVED_INTENTS"
    _still_locked(e, store)


def _accepted_exit(store, ex):
    e = _locked(store, ex)
    out = e.execute_reduce_only_order("BTCUSDT", "SELL", 0.1, 50000.0, validated_order=_tok("SELL", True))
    assert out["status"] == "SUCCESS"
    return e


def test_accepted_order_with_non_terminal_lifecycle_blocks_reset(store):
    ex = PaperExchange(0.1)
    e = _accepted_exit(store, ex)
    ex.positions["BTCUSDT"] = 0.0                    # flat, but the order's lifecycle is still NEW
    ex.query_status_override, ex.query_executed_override = "NEW", 0.0
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "IN_FLIGHT_ORDERS"
    _still_locked(e, store)


def test_lifecycle_unqueryable_blocks_reset(store):
    ex = PaperExchange(0.1)
    e = _accepted_exit(store, ex)
    ex.positions["BTCUSDT"] = 0.0
    ex.query_mode = "raise"
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "IN_FLIGHT_ORDERS"
    _still_locked(e, store)


def test_settled_terminal_lifecycle_allows_reset(store):
    ex = PaperExchange(0.1)
    e = _accepted_exit(store, ex)                    # PaperExchange fully filled it; position now 0
    r = e.manual_operator_reset()
    assert r["status"] == "SUCCESS", r
    from src.orders.pg_store import IntentState
    assert store.list_by_state(IntentState.ACCEPTED)[0].exchange_status == "FILLED"


# ------------------------------------------------------------ missing / error / stale => keep lock
@pytest.mark.parametrize("attr", ["positions_mode", "open_orders_mode", "conditional_mode"])
@pytest.mark.parametrize("mode", ["raise", "stale", "incomplete"])
def test_missing_stale_or_failed_query_keeps_lock(store, attr, mode):
    ex = PaperExchange(0.0)
    setattr(ex, attr, mode)
    e = _locked(store, ex)
    store.freeze_entries("test")
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["verification_incomplete"] is True
    assert r["kind"] == "INCOMPLETE_VERIFICATION"
    _still_locked(e, store)


def test_lock_survives_restart_after_refused_reset(store, pg_dsn):
    ex = PaperExchange(0.0)
    ex.positions_mode = "raise"
    e = _locked(store, ex)
    store.freeze_entries("test")
    assert e.manual_operator_reset()["status"] == "FAILED"
    e2 = ExecutionEngine(order_store=OrderStore(pg_dsn))      # "restart"
    assert e2.new_entry_blocked is True


# ------------------------------------------------------------ delayed fill around the release
def test_delayed_fill_between_the_two_pre_release_passes_refuses(store):
    ex = PaperExchange(0.0)
    e = _locked(store, ex)
    store.freeze_entries("test")

    def hook(kind, n):
        if kind == "positions" and n == 1:           # pass 1 saw flat; the fill lands right after
            ex.positions["BTCUSDT"] = 0.2
    ex.on_account_query = hook
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "EXPOSURE_PRESENT"
    _still_locked(e, store)


def test_delayed_fill_right_after_release_is_caught_and_entries_refrozen(store):
    ex = PaperExchange(0.0)
    e = _locked(store, ex)
    store.freeze_entries("test")

    def hook(kind, n):
        if kind == "positions" and n == 2:           # pass 2 saw flat; fill lands before release
            ex.positions["BTCUSDT"] = 0.2
    ex.on_account_query = hook
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "POST_RELEASE_CHANGE"
    assert r["underlying_kind"] == "EXPOSURE_PRESENT" and r["entries_refrozen"] is True
    assert e.new_entry_blocked is True
    _still_locked(e, store)
    _entry_auth(store, "a2")
    with pytest.raises(EntriesBlocked):              # the DB gate really is closed again
        store.consume_persist_and_claim("a2")


def test_new_intent_appearing_right_after_release_is_caught_and_refrozen(store):
    ex = PaperExchange(0.0)
    e = _locked(store, ex)
    store.freeze_entries("test")
    _entry_auth(store, "late")
    real_release = store.release_entry_freeze

    def release_then_entry_slips_in():
        ok = real_release()
        store.consume_persist_and_claim("late")      # gate is open for an instant: an ENTRY wins it
        return ok
    store.release_entry_freeze = release_then_entry_slips_in
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "POST_RELEASE_CHANGE"
    assert r["underlying_kind"] == "UNRESOLVED_INTENTS"
    _still_locked(e, store)
    # RESIDUAL (documented, not solved): that ENTRY already holds submit authority and may be
    # sent; re-freezing stops only the NEXT ones.


def test_intent_appearing_after_last_check_is_refused_by_the_db_release_guard(store):
    ex = PaperExchange(0.0)
    e = _locked(store, ex)
    store.freeze_entries("test")
    _entry_auth(store, "race")

    def hook(kind, n):
        if kind == "open_orders" and n == 2:         # after pass 2's checks, before the release
            store.consume_and_persist_intent("race")  # unresolved INTENT_PERSISTED appears
    ex.on_account_query = hook
    r = e.manual_operator_reset()
    assert r["status"] == "FAILED" and r["kind"] == "UNRESOLVED_INTENTS"
    _still_locked(e, store)


def test_settle_delay_is_applied_between_passes(store, monkeypatch):
    import src.core.execution_engine as mod
    sleeps = []
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))
    ex = PaperExchange(0.0)
    e = _locked(store, ex)
    e.reset_settle_s = 1.5
    assert e.manual_operator_reset()["status"] == "SUCCESS"
    assert sleeps == [1.5]


def test_success_reports_the_residual_risk_it_cannot_close(store):
    r = _locked(store, PaperExchange(0.0)).manual_operator_reset()
    assert r["status"] == "SUCCESS"
    assert "orders_in_flight_after_post_release_check" in r["unverified"]
