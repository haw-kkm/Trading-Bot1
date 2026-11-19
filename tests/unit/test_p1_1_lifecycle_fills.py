"""P1-1: submission result vs exchange lifecycle vs cumulative fills (real PostgreSQL).
All response shapes are an ASSUMED contract; Toobit's real format is UNVERIFIED."""
import threading

import psycopg
import pytest

from src.core.order_guardian import OrderGuardian
from src.orders.errors import ExchangeRejected
from src.orders.pg_store import IntentState, OrderStore
from src.orders.submitter import SafeSubmitter

QTY = 1.0


def _mk(store, sender, query, n=[0]):
    n[0] += 1
    aid = f"auth-{n[0]}-{id(sender)}"
    store.issue_authorization(aid, "ENTRY", "BTCUSDT", "BUY", "MARKET", QTY, None, False)
    sub = SafeSubmitter(store, sender, query)
    return sub, aid


def _timeout(_intent):
    raise TimeoutError("lost")


def _resp(intent, status, executed=None, fills=None, **kw):
    r = {"clientOrderId": intent.client_order_id, "orderId": "X1", "status": status,
         "symbol": "BTCUSDT", "side": "BUY", "origQty": QTY}
    if executed is not None:
        r["executedQty"] = executed
    if fills is not None:
        r["fills"] = fills
    r.update(kw)
    return r


def _unknown(store, query_holder):
    sub, aid = _mk(store, _timeout, lambda cid: query_holder["resp"](cid))
    res = sub.submit(aid)
    assert res.state == IntentState.SUBMISSION_UNKNOWN
    return sub, res.intent


def _q(store, intent, **kw):
    holder = {}
    return holder


def _setup(store, status, executed=None, fills=None, **kw):
    holder = {}
    sub, intent = _unknown(store, holder)
    holder["resp"] = lambda cid: _resp(intent, status, executed, fills, **kw)
    return sub, intent


# ----------------------------------------------------------- submission result vs lifecycle
@pytest.mark.parametrize("status,executed", [("CANCELED", 0.4), ("EXPIRED", 0.4), ("CANCELED", 0.0),
                                             ("EXPIRED", 0.0), ("FILLED", 1.0), ("NEW", None)])
def test_lifecycle_outcomes_resolve_to_ACCEPTED_not_REJECTED(store, status, executed):
    sub, intent = _setup(store, status, executed)
    out = sub.reconcile(intent.intent_id)
    assert out.state == IntentState.ACCEPTED
    assert out.exchange_status == status
    assert out.filled_qty == pytest.approx(executed or 0.0)


def test_canceled_after_partial_fill_keeps_the_partial_fill(store):
    sub, intent = _setup(store, "CANCELED", 0.3)
    out = sub.reconcile(intent.intent_id)
    assert (out.state, out.exchange_status, out.filled_qty) == (IntentState.ACCEPTED, "CANCELED", pytest.approx(0.3))


@pytest.mark.parametrize("status", ["CANCELED", "EXPIRED", "PARTIALLY_FILLED", "FILLED"])
def test_terminal_status_without_cumulative_qty_stays_unknown(store, status):
    sub, intent = _setup(store, status, None)
    assert sub.reconcile(intent.intent_id).state == IntentState.SUBMISSION_UNKNOWN


def test_rejected_only_when_proven_zero_fill(store):
    sub, intent = _setup(store, "REJECTED", 0.0)
    assert sub.reconcile(intent.intent_id).state == IntentState.REJECTED


@pytest.mark.parametrize("executed", [None, 0.2])
def test_rejected_with_fill_or_unknown_qty_is_not_proof_of_rejection(store, executed):
    sub, intent = _setup(store, "REJECTED", executed)
    assert sub.reconcile(intent.intent_id).state == IntentState.SUBMISSION_UNKNOWN


@pytest.mark.parametrize("status,executed", [("FILLED", 0.5), ("PARTIALLY_FILLED", 0.0),
                                             ("FILLED", 2.0), ("WEIRD", 0.0)])
def test_inconsistent_or_unknown_status_stays_unknown(store, status, executed):
    sub, intent = _setup(store, status, executed)
    assert sub.reconcile(intent.intent_id).state == IntentState.SUBMISSION_UNKNOWN


def test_fills_exceeding_cumulative_is_inconsistent(store):
    sub, intent = _setup(store, "CANCELED", 0.1, fills=[{"fillId": "a", "qty": 0.5, "price": 1}])
    assert sub.reconcile(intent.intent_id).state == IntentState.SUBMISSION_UNKNOWN


@pytest.mark.parametrize("resp", [{}, None, {"status": "OK"}, {"code": 0}, "garbage", {"orderId": ""}])
def test_unclear_submit_response_is_UNKNOWN_never_REJECTED(store, resp):
    sub, aid = _mk(store, lambda i: resp, None)
    assert sub.submit(aid).state == IntentState.SUBMISSION_UNKNOWN


def test_only_definitive_rejection_is_REJECTED(store):
    def rej(_i):
        raise ExchangeRejected("insufficient margin")
    sub, aid = _mk(store, rej, None)
    assert sub.submit(aid).state == IntentState.REJECTED


def test_submit_ack_carrying_partial_fill_is_persisted_separately(store):
    def send(i):
        return {"orderId": "9", "status": "PARTIALLY_FILLED", "executedQty": 0.25,
                "clientOrderId": i.client_order_id}
    sub, aid = _mk(store, send, None)
    r = sub.submit(aid)
    assert r.state == IntentState.ACCEPTED
    assert (r.intent.exchange_status, r.intent.filled_qty) == ("PARTIALLY_FILLED", pytest.approx(0.25))


# ----------------------------------------------------------- post-cancel partial fill
def test_partial_fill_before_cancel_is_reflected_after_the_fact(store):
    state = {"resp": None}
    sub, aid = _mk(store, lambda i: {"orderId": "1", "status": "SUCCESS", "clientOrderId": i.client_order_id},
                   lambda cid: state["resp"])
    r = sub.submit(aid)
    assert r.state == IntentState.ACCEPTED and r.intent.filled_qty == 0
    state["resp"] = _resp(r.intent, "CANCELED", 0.3, fills=[{"fillId": "t1", "qty": 0.3, "price": 100}])
    out = sub.refresh_lifecycle(r.intent.intent_id)
    assert (out.state, out.exchange_status, out.filled_qty) == (IntentState.ACCEPTED, "CANCELED", pytest.approx(0.3))
    assert sub.store.list_lifecycle_open() == []         # terminal => no longer polled


# ----------------------------------------------------------- idempotency / monotonicity
def _accepted(store):
    sub, aid = _mk(store, lambda i: {"orderId": "1", "status": "SUCCESS", "clientOrderId": i.client_order_id}, None)
    return sub.submit(aid).intent


def test_duplicate_fill_event_is_not_double_counted(store):
    it = _accepted(store)
    f = [{"fill_id": "t1", "qty": 0.4, "price": 10}]
    a = store.record_lifecycle(it.intent_id, "PARTIALLY_FILLED", None, f)
    b = store.record_lifecycle(it.intent_id, "PARTIALLY_FILLED", None, f)
    assert a["filled_qty"] == pytest.approx(0.4) and a["new_fills"] == 1
    assert b["filled_qty"] == pytest.approx(0.4) and b["new_fills"] == 0


def test_concurrent_duplicate_fill_delivery_counts_once(store):
    it = _accepted(store)
    bar, outs = threading.Barrier(8), []

    def w():
        bar.wait()
        outs.append(store.record_lifecycle(it.intent_id, "PARTIALLY_FILLED", None,
                                           [{"fill_id": "same", "qty": 0.2, "price": 1}]))
    ts = [threading.Thread(target=w) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(o["new_fills"] for o in outs) == 1
    assert store.get(it.intent_id).filled_qty == pytest.approx(0.2)


def test_distinct_fills_accumulate(store):
    it = _accepted(store)
    store.record_lifecycle(it.intent_id, "PARTIALLY_FILLED", None, [{"fill_id": "a", "qty": 0.2}])
    r = store.record_lifecycle(it.intent_id, "PARTIALLY_FILLED", None,
                               [{"fill_id": "a", "qty": 0.2}, {"fill_id": "b", "qty": 0.3}])
    assert r["filled_qty"] == pytest.approx(0.5)


def test_stale_lower_cumulative_never_shrinks_fill_and_terminal_never_reverts(store):
    it = _accepted(store)
    store.record_lifecycle(it.intent_id, "CANCELED", 0.6)
    r = store.record_lifecycle(it.intent_id, "NEW", 0.1)          # late, stale report
    assert r["filled_qty"] == pytest.approx(0.6) and r["exchange_status"] == "CANCELED"


def test_db_trigger_refuses_filled_qty_decrease_even_via_raw_sql(store, pg_dsn):
    it = _accepted(store)
    store.record_lifecycle(it.intent_id, "PARTIALLY_FILLED", 0.5)
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        with pytest.raises(psycopg.errors.CheckViolation):
            c.execute("UPDATE order_intents SET filled_qty=0.1 WHERE intent_id=%s", (it.intent_id,))
        with pytest.raises(psycopg.errors.UniqueViolation):
            c.execute("INSERT INTO order_fills(intent_id, exchange_fill_id, qty) VALUES (%s,'z',0.1)", (it.intent_id,))
            c.execute("INSERT INTO order_fills(intent_id, exchange_fill_id, qty) VALUES (%s,'z',0.1)", (it.intent_id,))


def test_overfill_and_status_mismatch_are_flagged_not_hidden(store):
    it = _accepted(store)
    r = store.record_lifecycle(it.intent_id, "FILLED", 1.5)
    assert "OVERFILL" in r["flags"] and r["filled_qty"] == pytest.approx(1.5)
    it2 = _accepted(store)
    r2 = store.record_lifecycle(it2.intent_id, "FILLED", 0.5)
    assert "FILLED_STATUS_QTY_MISMATCH" in r2["flags"]


def test_fill_on_rejected_intent_is_recorded_and_flagged(store):
    def rej(_i):
        raise ExchangeRejected("no")
    sub, aid = _mk(store, rej, None)
    it = sub.submit(aid).intent
    r = store.record_lifecycle(it.intent_id, "FILLED", 1.0)
    assert "FILL_ON_REJECTED" in r["flags"] and store.get(it.intent_id).state == IntentState.REJECTED


# ----------------------------------------------------------- restart recovery
def test_restart_recovers_unknown_and_applies_partial_fill_from_canceled_order(store, pg_dsn):
    holder = {}
    sub, intent = _unknown(store, holder)
    # "process dies": brand-new store + submitter objects on the same DB
    store2 = OrderStore(pg_dsn)
    holder2 = {"resp": lambda cid: _resp(intent, "EXPIRED", 0.35,
                                         fills=[{"fillId": "f1", "qty": 0.35, "price": 5}])}
    sub2 = SafeSubmitter(store2, _timeout, lambda cid: holder2["resp"](cid))
    sub2.recover_on_startup()
    done = sub2.reconcile_all()
    assert [(d.state, d.exchange_status, d.filled_qty) for d in done] == [
        (IntentState.ACCEPTED, "EXPIRED", pytest.approx(0.35))]
    sub2.reconcile_all()                                      # re-running is idempotent
    assert store2.get(intent.intent_id).filled_qty == pytest.approx(0.35)
    assert store2.entries_frozen()                            # resolution still does not unfreeze


def test_refresh_all_lifecycles_updates_open_accepted_orders(store):
    holder = {}
    sub, aid = _mk(store, lambda i: {"orderId": "1", "status": "SUCCESS", "clientOrderId": i.client_order_id},
                   lambda cid: holder["r"])
    it = sub.submit(aid).intent
    holder["r"] = _resp(it, "PARTIALLY_FILLED", 0.2)
    sub.refresh_all_lifecycles()
    assert store.get(it.intent_id).filled_qty == pytest.approx(0.2)
    holder["r"] = _resp(it, "FILLED", 1.0)
    sub.refresh_all_lifecycles()
    assert store.get(it.intent_id).exchange_status == "FILLED" and store.list_lifecycle_open() == []


# ----------------------------------------------------------- guardian position idempotency
def test_guardian_duplicate_fill_event_does_not_double_count_position():
    g = OrderGuardian()
    o = g.create_order("BTCUSDT", "BUY", 1.0, 50000.0)
    g.submit_order(o["order_id"])
    g.open_order(o["order_id"])
    r1 = g.handle_fill(o["order_id"], 0.4, 50000.0, fill_id="t1")
    r2 = g.handle_fill(o["order_id"], 0.4, 50000.0, fill_id="t1")        # re-delivered
    assert r2["status"] == "DUPLICATE_FILL_IGNORED"
    assert g.get_order(o["order_id"])["filled_quantity"] == pytest.approx(0.4)
    g.handle_fill(o["order_id"], 0.3, 50000.0, fill_id="t2")
    assert g.get_order(o["order_id"])["filled_quantity"] == pytest.approx(0.7)
    assert r1["status"] != "DUPLICATE_FILL_IGNORED"
