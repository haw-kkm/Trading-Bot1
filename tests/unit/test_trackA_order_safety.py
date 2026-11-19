"""Track A: local duplicate-execution safety. Mock-only; no Toobit/network access."""

import os
import sqlite3
import subprocess
import sys
import threading

import pytest

from src.core.order_store import (
    AuthorizationAlreadyConsumed, IllegalTransition, IntentState, OrderStore,
)
from src.core.safe_submitter import ExchangeRejected, SafeSubmitter

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockClient:
    def __init__(self, submit=None, query=None):
        self.submit_calls = []
        self.query_calls = 0
        self._submit = submit
        self._query = query

    def submit_order(self, cid, symbol, side, qty, price):
        self.submit_calls.append(cid)
        if self._submit:
            return self._submit(cid)
        return {"orderId": "X1", "clientOrderId": cid}

    def query_order(self, cid):
        self.query_calls += 1
        if self._query:
            return self._query(cid)
        return {}


@pytest.fixture
def store(tmp_path):
    s = OrderStore(str(tmp_path / "orders.db"))
    s.issue_authorization("A1", "BTCUSDT", "BUY", 0.5, 50000.0)
    return s


def _timeout(_cid):
    raise TimeoutError("read timed out")


def _unknown_intent(store):
    client = MockClient(submit=_timeout)
    it = SafeSubmitter(store, client).authorize_and_submit("A1")
    assert it.state == IntentState.SUBMISSION_UNKNOWN
    return it, client


# ---- (1)(3) duplicate receipt / concurrency ---------------------------------
def test_duplicate_request_consumes_once(store):
    client = MockClient()
    sub = SafeSubmitter(store, client)
    assert sub.authorize_and_submit("A1").state == IntentState.ACCEPTED
    with pytest.raises(AuthorizationAlreadyConsumed):
        sub.authorize_and_submit("A1")
    assert len(client.submit_calls) == 1
    assert store.count_intents("A1") == 1


def test_unknown_authorization_rejected(store):
    with pytest.raises(AuthorizationAlreadyConsumed):
        store.consume_and_persist_intent("NOPE")


def test_concurrent_threads_single_winner(store):
    client = MockClient()
    sub = SafeSubmitter(store, client)
    wins, losses, barrier = [], [], threading.Barrier(16)

    def work():
        barrier.wait()
        try:
            wins.append(sub.authorize_and_submit("A1"))
        except AuthorizationAlreadyConsumed:
            losses.append(1)

    ts = [threading.Thread(target=work) for _ in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(wins) == 1 and len(losses) == 15
    assert len(client.submit_calls) == 1
    assert store.count_intents() == 1


_WORKER = """
import sys
from src.core.order_store import OrderStore, AuthorizationAlreadyConsumed
s = OrderStore(sys.argv[1])
try:
    s.consume_and_persist_intent("A1"); print("WIN")
except AuthorizationAlreadyConsumed:
    print("LOSE")
"""


def test_concurrent_processes_single_winner(store):
    procs = [subprocess.Popen([sys.executable, "-I", "-c", "import sys;sys.path.insert(0,%r)\n%s" % (ROOT, _WORKER),
                               store.db_path], stdout=subprocess.PIPE, text=True, cwd=ROOT)
             for _ in range(6)]
    outs = [p.communicate()[0].strip() for p in procs]
    assert outs.count("WIN") == 1 and outs.count("LOSE") == 5
    assert store.count_intents("A1") == 1


def test_db_unique_constraint_blocks_second_intent_row(store):
    """Even bypassing app logic, the DB refuses a 2nd intent for one authorization."""
    store.consume_and_persist_intent("A1")
    conn = sqlite3.connect(store.db_path)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO order_intents(authorization_id, client_order_id, symbol, side, quantity,"
            " state, created_at, updated_at) VALUES ('A1','other','BTCUSDT','BUY',1,"
            "'INTENT_PERSISTED',0,0)")
    conn.close()
    assert store.count_intents("A1") == 1


def test_retry_after_restart_cannot_duplicate(store):
    store.consume_and_persist_intent("A1")
    store2 = OrderStore(store.db_path)  # "restart"
    with pytest.raises(AuthorizationAlreadyConsumed):
        store2.consume_and_persist_intent("A1")
    assert store2.count_intents("A1") == 1


# ---- (2) persistence before submit; DB failure/rollback ----------------------
def test_db_failure_after_consume_rolls_back_and_no_submit(store):
    def boom(point):
        if point == "after_consume":
            raise sqlite3.OperationalError("disk I/O error")
    store._fault = boom
    client = MockClient()
    with pytest.raises(sqlite3.OperationalError):
        SafeSubmitter(store, client).authorize_and_submit("A1")
    assert client.submit_calls == []
    assert store.authorization_status("A1") == "UNUSED"  # consume rolled back
    assert store.count_intents() == 0
    store._fault = None
    assert SafeSubmitter(store, client).authorize_and_submit("A1").state == IntentState.ACCEPTED


def test_db_failure_after_intent_insert_rolls_back_both(store):
    def boom(point):
        if point == "after_intent_insert":
            raise RuntimeError("commit failed")
    store._fault = boom
    client = MockClient()
    with pytest.raises(RuntimeError):
        SafeSubmitter(store, client).authorize_and_submit("A1")
    assert client.submit_calls == [] and store.count_intents() == 0
    assert store.authorization_status("A1") == "UNUSED"


def test_claim_failure_prevents_network_call(store):
    """If the SUBMITTING claim cannot be persisted, the client is never called."""
    it = store.consume_and_persist_intent("A1")
    conn = sqlite3.connect(store.db_path)
    conn.execute("CREATE TRIGGER fail_claim BEFORE UPDATE ON order_intents "
                 "BEGIN SELECT RAISE(ABORT,'db down'); END")
    conn.commit(); conn.close()
    client = MockClient()
    with pytest.raises(sqlite3.DatabaseError):
        SafeSubmitter(store, client).submit_intent(it.intent_id)
    assert client.submit_calls == []


def _run_crash(db, mode):
    code = (
        "import os,sys\nsys.path.insert(0,%r)\n"
        "from src.core.order_store import OrderStore, IntentState\n"
        "from src.core.safe_submitter import SafeSubmitter\n"
        "s=OrderStore(sys.argv[1]); it=s.consume_and_persist_intent('A1')\n"
        "if sys.argv[2]=='after_persist': os._exit(9)\n"
        "class C:\n"
        "    def submit_order(self,*a): os._exit(9)\n"
        "SafeSubmitter(s,C()).submit_intent(it.intent_id)\n" % ROOT
    )
    return subprocess.run([sys.executable, "-I", "-c", code, db, mode], cwd=ROOT).returncode


# ---- crash / restart -----------------------------------------------------------
def test_crash_right_after_persist_is_recoverable_not_unknown(store):
    assert _run_crash(store.db_path, "after_persist") == 9
    s2 = OrderStore(store.db_path)
    pending = s2.recover_on_startup()
    assert [p.state for p in pending] == [IntentState.INTENT_PERSISTED]
    assert s2.authorization_status("A1") == "CONSUMED"
    with pytest.raises(AuthorizationAlreadyConsumed):
        s2.consume_and_persist_intent("A1")
    client = MockClient()  # explicit, caller-driven submission of the recovered intent
    assert SafeSubmitter(s2, client).submit_intent(pending[0].intent_id).state == IntentState.ACCEPTED
    assert len(client.submit_calls) == 1


def test_crash_during_send_restores_as_unknown_and_never_resent(store):
    assert _run_crash(store.db_path, "during_send") == 9
    s2 = OrderStore(store.db_path)
    pending = s2.recover_on_startup()
    assert pending == []
    unk = s2.list_by_state(IntentState.SUBMISSION_UNKNOWN)
    assert len(unk) == 1 and unk[0].last_error == "crash_during_submit"
    client = MockClient()
    sub = SafeSubmitter(s2, client)
    with pytest.raises(IllegalTransition):
        sub.submit_intent(unk[0].intent_id)
    s2.recover_on_startup()  # idempotent, still no resubmission
    assert client.submit_calls == []
    assert s2.get(unk[0].intent_id).state == IntentState.SUBMISSION_UNKNOWN


# ---- (4) UNKNOWN on timeout / lost response / bad response ----------------------
def test_timeout_persists_submission_unknown(store):
    it, client = _unknown_intent(store)
    assert OrderStore(store.db_path).get(it.intent_id).state == IntentState.SUBMISSION_UNKNOWN
    assert len(client.submit_calls) == 1


@pytest.mark.parametrize("resp", [None, {}, {"orderId": "1"}, {"orderId": "", "clientOrderId": "auth-A1"},
                                  {"orderId": "1", "clientOrderId": "someone-else"}])
def test_unusable_response_is_unknown(store, resp):
    it = SafeSubmitter(store, MockClient(submit=lambda c: resp)).authorize_and_submit("A1")
    assert it.state == IntentState.SUBMISSION_UNKNOWN


def test_explicit_rejection_is_rejected_not_unknown(store):
    def rej(_c):
        raise ExchangeRejected("insufficient margin")
    it = SafeSubmitter(store, MockClient(submit=rej)).authorize_and_submit("A1")
    assert it.state == IntentState.REJECTED


def test_state_machine_blocks_illegal_transitions(store):
    it, _ = _unknown_intent(store)
    for bad in (IntentState.INTENT_PERSISTED, IntentState.SUBMITTING):
        with pytest.raises(IllegalTransition):
            store.transition(it.intent_id, IntentState.SUBMISSION_UNKNOWN, bad)
    # DB-level trigger also blocks raw writers
    conn = sqlite3.connect(store.db_path)
    with pytest.raises(sqlite3.DatabaseError):
        conn.execute("UPDATE order_intents SET state='INTENT_PERSISTED' WHERE intent_id=?", (it.intent_id,))
    conn.close()


# ---- (5)(6) no auto-resubmit; reconciliation safety -----------------------------
def test_no_auto_resubmit_paths(store):
    it, client = _unknown_intent(store)
    sub = SafeSubmitter(store, client)
    with pytest.raises(IllegalTransition):
        sub.submit_intent(it.intent_id)           # direct retry
    store.recover_on_startup()                    # restart path
    sub.reconcile_all()                           # reconcile path (query returns {})
    assert len(client.submit_calls) == 1


@pytest.mark.parametrize("resp", [
    {},                                                     # empty / "not found"
    {"code": -2013, "msg": "Order does not exist"},         # not-found error body
    {"clientOrderId": "auth-A1", "status": "NEW"},          # incomplete
    {"clientOrderId": "auth-A1", "orderId": "9", "status": "WEIRD",
     "symbol": "BTCUSDT", "side": "BUY"},                   # unknown status
    {"clientOrderId": "other", "orderId": "9", "status": "NEW",
     "symbol": "BTCUSDT", "side": "BUY"},                   # id mismatch
    {"clientOrderId": "auth-A1", "orderId": "9", "status": "NEW",
     "symbol": "ETHUSDT", "side": "BUY"},                   # symbol conflict
    {"clientOrderId": "auth-A1", "orderId": "9", "status": "FILLED",
     "symbol": "BTCUSDT", "side": "BUY", "origQty": "7"},   # qty conflict
    None, "garbage",
])
def test_inconclusive_reconcile_keeps_unknown(store, resp):
    it, _ = _unknown_intent(store)
    client = MockClient(query=lambda c: resp)
    out = SafeSubmitter(store, client).reconcile(it.intent_id)
    assert out.state == IntentState.SUBMISSION_UNKNOWN and client.submit_calls == []


def test_reconcile_query_error_keeps_unknown(store):
    it, _ = _unknown_intent(store)

    def err(_c):
        raise ConnectionError("down")
    out = SafeSubmitter(store, MockClient(query=err)).reconcile(it.intent_id)
    assert out.state == IntentState.SUBMISSION_UNKNOWN


def test_reconcile_positive_evidence_resolves(store):
    it, _ = _unknown_intent(store)
    ok = {"clientOrderId": "auth-A1", "orderId": "77", "status": "NEW",
          "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.5"}
    out = SafeSubmitter(store, MockClient(query=lambda c: ok)).reconcile(it.intent_id)
    assert out.state == IntentState.ACCEPTED and out.exchange_order_id == "77"


def test_reconcile_explicit_exchange_rejection_resolves(store):
    it, _ = _unknown_intent(store)
    r = {"clientOrderId": "auth-A1", "orderId": "77", "status": "REJECTED",
         "symbol": "BTCUSDT", "side": "BUY"}
    out = SafeSubmitter(store, MockClient(query=lambda c: r)).reconcile(it.intent_id)
    assert out.state == IntentState.REJECTED


def test_unknown_state_survives_restart_and_reconcile_after_restart(store):
    it, _ = _unknown_intent(store)
    s2 = OrderStore(store.db_path)
    s2.recover_on_startup()
    got = s2.get(it.intent_id)
    assert got.state == IntentState.SUBMISSION_UNKNOWN and "TimeoutError" in got.last_error
