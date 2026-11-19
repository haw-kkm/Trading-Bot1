"""Track A on REAL PostgreSQL (no DB mocks). Only the exchange network layer is mocked.

Covers: multi-process concurrent submission, token reuse, process kill, API response loss,
SUBMISSION_UNKNOWN recovery, emergency-liquidation de-duplication, DB constraints/rollback,
no transaction held during the exchange call, purpose policy, live-trading gate.
"""

import os
import subprocess
import sys
import threading

import psycopg
import pytest

from src.core.execution_engine import EmergencyLockState, ExecutionEngine
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine
from src.orders.errors import (
    ActiveEmergencyOrderExists, AuthorizationAlreadyConsumed, EntriesBlocked, ExchangeRejected,
    IllegalTransition, PersistenceFailed,
)
from src.orders.pg_store import IntentState, OrderStore
from src.orders.submitter import SafeSubmitter

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------- helpers
class FakeExchange:
    """Mock network layer. is_simulated lets it pass the live-trading gate."""
    is_simulated = True

    def __init__(self, fail=None, query=None, ioc_fill=True):
        self.sent = []          # (kind, client_order_id)
        self.fail = fail        # exception instance to raise on every send
        self._query = query
        self.ioc_fill = ioc_fill
        self.queries = 0

    def _do(self, kind, cid):
        self.sent.append((kind, cid))
        if self.fail:
            raise self.fail
        return {"status": "SUCCESS", "orderId": f"X{len(self.sent)}", "clientOrderId": cid,
                "fill_status": "FILLED" if self.ioc_fill else "UNFILLED"}

    def execute_market_order(self, symbol, side, quantity, client_order_id=None, reduce_only=False):
        return self._do("market", client_order_id)

    def execute_ioc_order(self, symbol, side, quantity, price, client_order_id=None, reduce_only=False):
        return self._do("ioc", client_order_id)

    def execute_reduce_only_order(self, symbol, side, quantity, price, order_type="LIMIT",
                                  client_order_id=None, reduce_only=True):
        return self._do("reduce", client_order_id)

    def query_order(self, cid):
        self.queries += 1
        return self._query(cid) if self._query else {}

    def register_sl_tp(self, *a):
        return {"status": "FAILED"}

    def get_position(self, symbol):
        return {"position_size": 0.0}

    def is_extreme_conditions(self):
        return False


def _token(side="BUY", qty=0.1):
    r = RiskEngine().validate_trade(TradeHypothesis(
        symbol="BTCUSDT", direction=side, quantity=qty, leverage=10.0, price=50000.0,
        stop_loss=49000.0, take_profit=51000.0))
    return r["validated_order"]


def _engine(store, client=None):
    e = ExecutionEngine(order_store=store)
    e.toobit_client = client or FakeExchange()
    return e


def _issue(store, aid="A1", purpose="ENTRY", reduce_only=False, symbol="BTCUSDT"):
    store.issue_authorization(aid, purpose, symbol, "BUY", "MARKET", 0.5, None, reduce_only)


def _unknown(store, aid="A1"):
    _issue(store, aid)
    ex = FakeExchange(fail=TimeoutError("read timed out"))
    sub = SafeSubmitter(store, lambda i: ex.execute_market_order(i.symbol, i.side, i.quantity,
                                                                 client_order_id=i.client_order_id),
                        query=ex.query_order)
    res = sub.submit(aid)
    assert res.state == IntentState.SUBMISSION_UNKNOWN
    return res.intent, sub, ex


def _run(code, *args):
    return subprocess.run(
        [sys.executable, "-I", "-c", "import sys;sys.path.insert(0,%r)\n%s" % (ROOT, code), *args],
        cwd=ROOT, capture_output=True, text=True)


# ------------------------------------------- 1/3: authorization + unique intent
def test_duplicate_request_consumes_once(store):
    _issue(store)
    sub = SafeSubmitter(store, lambda i: {"status": "SUCCESS"})
    assert sub.submit("A1").state == IntentState.ACCEPTED
    with pytest.raises(AuthorizationAlreadyConsumed):
        sub.submit("A1")
    assert store.count_intents("A1") == 1


def test_reissue_cannot_rearm_consumed_authorization(store):
    _issue(store)
    SafeSubmitter(store, lambda i: {"status": "SUCCESS"}).submit("A1")
    assert store.issue_authorization("A1", "ENTRY", "BTCUSDT", "BUY", "MARKET", 0.5, None, False) is False
    with pytest.raises(AuthorizationAlreadyConsumed):
        store.consume_and_persist_intent("A1")


def test_threads_single_winner_real_db_locking(store):
    _issue(store)
    sends, wins, barrier = [], [], threading.Barrier(12)
    sub = SafeSubmitter(store, lambda i: sends.append(i.client_order_id) or {"status": "SUCCESS"})

    def work():
        barrier.wait()
        try:
            wins.append(sub.submit("A1"))
        except AuthorizationAlreadyConsumed:
            pass

    ts = [threading.Thread(target=work) for _ in range(12)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(wins) == 1 and len(sends) == 1 and store.count_intents() == 1


_PROC = """
import sys
from src.orders.pg_store import OrderStore
from src.orders.errors import AuthorizationAlreadyConsumed
dsn, log = sys.argv[1], sys.argv[2]
def sender(i):
    with open(log, "a") as f: f.write("SEND\\n")
    return {"status": "SUCCESS", "orderId": "1"}
from src.orders.submitter import SafeSubmitter
try:
    SafeSubmitter(OrderStore(dsn), sender).submit("A1"); print("WIN")
except AuthorizationAlreadyConsumed:
    print("LOSE")
"""


def test_multi_process_concurrent_submission_sends_once(store, pg_dsn, tmp_path):
    _issue(store)
    log = tmp_path / "sends.log"
    procs = [subprocess.Popen([sys.executable, "-I", "-c",
                               "import sys;sys.path.insert(0,%r)\n%s" % (ROOT, _PROC), pg_dsn, str(log)],
                              stdout=subprocess.PIPE, text=True, cwd=ROOT) for _ in range(6)]
    outs = [p.communicate()[0].strip() for p in procs]
    assert outs.count("WIN") == 1 and outs.count("LOSE") == 5
    assert log.read_text().count("SEND") == 1
    assert store.count_intents("A1") == 1


def test_db_constraints_block_second_intent_and_illegal_updates(store, pg_dsn):
    _issue(store)
    it = store.consume_and_persist_intent("A1")
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        with pytest.raises(psycopg.errors.UniqueViolation):          # one intent per authorization
            c.execute("INSERT INTO order_intents(authorization_id,client_order_id,purpose,symbol,side,"
                      "order_type,quantity,reduce_only,state) VALUES ('A1','zz','ENTRY','BTCUSDT','BUY',"
                      "'MARKET',1,false,'INTENT_PERSISTED')")
        with pytest.raises(psycopg.errors.CheckViolation):           # entry cannot be reduce-only
            c.execute("INSERT INTO authorizations(authorization_id,purpose,symbol,side,order_type,"
                      "quantity,reduce_only) VALUES ('B','ENTRY','BTCUSDT','BUY','MARKET',1,true)")
        with pytest.raises(psycopg.errors.CheckViolation):           # exit must be reduce-only
            c.execute("INSERT INTO authorizations(authorization_id,purpose,symbol,side,order_type,"
                      "quantity,reduce_only) VALUES ('C','EXIT','BTCUSDT','BUY','MARKET',1,false)")
        with pytest.raises(psycopg.errors.CheckViolation):           # identity immutable
            c.execute("UPDATE order_intents SET quantity=99 WHERE intent_id=%s", (it.intent_id,))
        with pytest.raises(psycopg.errors.CheckViolation):           # skipping states
            c.execute("UPDATE order_intents SET state='ACCEPTED' WHERE intent_id=%s", (it.intent_id,))


def test_retry_after_restart_cannot_duplicate(store, pg_dsn):
    _issue(store)
    store.consume_and_persist_intent("A1")
    with pytest.raises(AuthorizationAlreadyConsumed):
        OrderStore(pg_dsn).consume_and_persist_intent("A1")  # fresh store object == restart


# ------------------------------------------------ 2: persist-before-send, rollback
def test_db_failure_rolls_back_and_nothing_sent(store):
    _issue(store)
    sends = []
    sub = SafeSubmitter(store, lambda i: sends.append(1))
    for point in ("after_consume", "after_intent_insert"):
        def boom(p, point=point):
            if p == point:
                raise psycopg.OperationalError("injected failure")
        store._fault = boom
        with pytest.raises(PersistenceFailed):
            sub.submit("A1")
        assert sends == [] and store.count_intents() == 0
        assert store.authorization_status("A1") == "UNUSED"   # consume rolled back with it
    store._fault = None
    assert sub.submit("A1").state == IntentState.ACCEPTED or sends


def test_no_db_transaction_or_lock_held_during_exchange_call(store, pg_dsn):
    _issue(store)
    seen = {}

    def sender(intent):
        with psycopg.connect(pg_dsn, autocommit=True) as c:
            c.execute("SET lock_timeout = '500ms'")
            with c.transaction():  # would block/fail if the claim tx or row lock were still open
                seen["row"] = c.execute("SELECT state FROM order_intents WHERE intent_id=%s FOR UPDATE NOWAIT",
                                        (intent.intent_id,)).fetchone()[0]
            seen["idle_in_tx"] = c.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE state LIKE 'idle in transaction%%' "
                "AND datname = current_database()").fetchone()[0]
        return {"status": "SUCCESS"}

    SafeSubmitter(store, sender).submit("A1")
    assert seen["row"] == "SUBMIT_PENDING"       # durable claim visible to others during the call
    assert seen["idle_in_tx"] == 0


# --------------------------------------- 4/5: UNKNOWN, no resend, process kill
@pytest.mark.parametrize("fail", [TimeoutError("t"), ConnectionResetError("c"), RuntimeError("x")])
def test_ambiguous_errors_persist_submission_unknown(store, fail):
    _issue(store)
    ex = FakeExchange(fail=fail)
    sub = SafeSubmitter(store, lambda i: ex.execute_market_order(i.symbol, i.side, i.quantity,
                                                                 client_order_id=i.client_order_id))
    assert sub.submit("A1").state == IntentState.SUBMISSION_UNKNOWN
    assert len(ex.sent) == 1


@pytest.mark.parametrize("resp", [None, {}, [], "ok", {"status": "FAILED"},
                                  {"orderId": "1", "clientOrderId": "someone-else"}])
def test_lost_or_unusable_response_is_unknown(store, resp):
    _issue(store)
    assert SafeSubmitter(store, lambda i: resp).submit("A1").state == IntentState.SUBMISSION_UNKNOWN


def test_definitive_rejection_is_rejected(store):
    _issue(store)

    def rej(i):
        raise ExchangeRejected("insufficient margin")
    assert SafeSubmitter(store, rej).submit("A1").state == IntentState.REJECTED


def test_process_kill_during_send_recovers_as_unknown_never_resent(store, pg_dsn):
    _issue(store)
    code = ("import os,sys\nfrom src.orders.pg_store import OrderStore\n"
            "from src.orders.submitter import SafeSubmitter\n"
            "SafeSubmitter(OrderStore(sys.argv[1]), lambda i: os._exit(9)).submit('A1')\n")
    assert _run(code, pg_dsn).returncode == 9
    s2 = OrderStore(pg_dsn)  # restart
    pending_row = s2.list_by_state(IntentState.SUBMIT_PENDING)
    assert len(pending_row) == 1                       # durable even though the process died
    sends = []
    sub = SafeSubmitter(s2, lambda i: sends.append(1), query=lambda c: {})
    assert sub.recover_on_startup(lease_s=0) == []     # nothing auto-submitted
    unk = s2.list_by_state(IntentState.SUBMISSION_UNKNOWN)
    assert len(unk) == 1 and unk[0].last_error == "claim_abandoned_or_process_died"
    with pytest.raises((IllegalTransition, EntriesBlocked)):   # state machine / entry gate
        sub.submit_persisted(unk[0].intent_id)
    sub.recover_on_startup(); sub.reconcile_all()
    assert sends == []
    assert s2.get(unk[0].intent_id).state == IntentState.SUBMISSION_UNKNOWN


def test_process_kill_after_persist_before_claim_is_recoverable(store, pg_dsn):
    _issue(store)
    code = ("import os,sys\nfrom src.orders.pg_store import OrderStore\n"
            "OrderStore(sys.argv[1]).consume_and_persist_intent('A1'); os._exit(9)\n")
    assert _run(code, pg_dsn).returncode == 9
    s2 = OrderStore(pg_dsn)
    sends = []
    sub = SafeSubmitter(s2, lambda i: sends.append(1) or {"status": "SUCCESS"})
    pending = sub.recover_on_startup()
    assert [p.state for p in pending] == [IntentState.INTENT_PERSISTED] and sends == []
    with pytest.raises(AuthorizationAlreadyConsumed):
        s2.consume_and_persist_intent("A1")
    assert sub.submit_persisted(pending[0].intent_id).state == IntentState.ACCEPTED
    assert len(sends) == 1


def test_stale_claim_cannot_overwrite_recovered_state(store):
    _issue(store)

    def sender(i):
        store.recover_stale_pending(0)   # someone else recovers our claim mid-call
        return {"status": "SUCCESS", "orderId": "1"}
    res = SafeSubmitter(store, sender).submit("A1")
    assert res.state == IntentState.SUBMISSION_UNKNOWN and res.recorded is False
    assert store.get(res.intent.intent_id).state == IntentState.SUBMISSION_UNKNOWN


def test_unknown_state_survives_restart(store, pg_dsn):
    it, _, _ = _unknown(store)
    got = OrderStore(pg_dsn).get(it.intent_id)
    assert got.state == IntentState.SUBMISSION_UNKNOWN and "TimeoutError" in got.last_error


def test_db_forbids_unknown_to_resubmittable_states(store, pg_dsn):
    it, _, _ = _unknown(store)
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        for bad in ("INTENT_PERSISTED", "SUBMIT_PENDING"):
            with pytest.raises(psycopg.errors.CheckViolation):
                c.execute("UPDATE order_intents SET state=%s WHERE intent_id=%s", (bad, it.intent_id))
        with pytest.raises(psycopg.errors.CheckViolation):  # resolution needs evidence
            c.execute("UPDATE order_intents SET state='ACCEPTED' WHERE intent_id=%s", (it.intent_id,))


# ----------------------------------------------------------- 6: reconcile
@pytest.mark.parametrize("resp", [
    {}, None, "garbage", {"code": -2013, "msg": "Order does not exist"},
    {"clientOrderId": "X", "status": "NEW"},
    {"orderId": "9", "status": "WEIRD", "symbol": "BTCUSDT", "side": "BUY"},
])
def test_inconclusive_reconcile_keeps_unknown(store, resp):
    it, sub, ex = _unknown(store)
    r = dict(resp) if isinstance(resp, dict) else resp
    if isinstance(r, dict) and r.get("clientOrderId") == "X":
        r["clientOrderId"] = it.client_order_id
    if isinstance(r, dict) and "orderId" in r and "clientOrderId" not in r:
        r["clientOrderId"] = it.client_order_id
    sub.query = lambda c: r
    assert sub.reconcile(it.intent_id).state == IntentState.SUBMISSION_UNKNOWN
    assert len(ex.sent) == 1


def test_reconcile_conflicts_and_errors_keep_unknown(store):
    it, sub, ex = _unknown(store)
    base = {"clientOrderId": it.client_order_id, "orderId": "9", "status": "NEW",
            "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.5"}
    for bad in ({**base, "symbol": "ETHUSDT"}, {**base, "origQty": "7"},
                {**base, "clientOrderId": "other"}, {**base, "side": "SELL"}):
        sub.query = lambda c, b=bad: b
        assert sub.reconcile(it.intent_id).state == IntentState.SUBMISSION_UNKNOWN

    def err(c):
        raise ConnectionError("down")
    sub.query = err
    assert sub.reconcile(it.intent_id).state == IntentState.SUBMISSION_UNKNOWN


def test_reconcile_positive_evidence_resolves_with_evidence(store):
    it, sub, _ = _unknown(store)
    sub.query = lambda c: {"clientOrderId": it.client_order_id, "orderId": "77", "status": "NEW",
                           "symbol": "BTCUSDT", "side": "BUY", "origQty": "0.5"}
    out = sub.reconcile(it.intent_id)
    assert out.state == IntentState.ACCEPTED and out.exchange_order_id == "77"


# ------------------------------------------------------ token reuse (engine)
def test_token_reuse_is_blocked_and_sends_once(store):
    client = FakeExchange()
    e = _engine(store, client)
    tok = _token()
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)["status"] == "SUCCESS"
    again = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)
    other_path = e.execute_ioc_order("BTCUSDT", "BUY", 0.1, 50000.0, validated_order=tok)
    assert again["status"] == other_path["status"] == "DUPLICATE_SUPPRESSED"
    assert len(client.sent) == 1


def test_token_reuse_across_engine_instances_and_threads(store):
    tok, client = _token(), FakeExchange()
    results, barrier = [], threading.Barrier(8)

    def work():
        e = _engine(store, client)
        barrier.wait()
        results.append(e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)["status"])

    ts = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert results.count("SUCCESS") == 1 and results.count("DUPLICATE_SUPPRESSED") == 7
    assert len(client.sent) == 1


def test_identical_trades_with_distinct_tokens_both_execute(store):
    client = FakeExchange()
    e = _engine(store, client)
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())["status"] == "SUCCESS"
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())["status"] == "SUCCESS"
    assert len(client.sent) == 2


# ------------------------------------------ engine: timeout is not failure
def test_engine_timeout_no_rollback_no_resend_and_blocks_entries(store):
    client = FakeExchange(fail=TimeoutError("timeout"))
    e = _engine(store, client)
    tok = _token()
    out = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)
    assert out["status"] == "SUBMISSION_UNKNOWN" and getattr(e, "rollback_calls", 0) == 0
    retry = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)   # blocked by freeze
    assert retry["status"] == "REJECTED" and "freeze" in retry["reason"]
    e.new_entry_blocked = False   # in-memory flag cleared: the PERSISTED gate still refuses
    again = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)
    assert again["status"] == "REJECTED" and "entry gate closed (DB)" in again["reason"]
    assert store.authorization_status(tok.authorization_id) == "CONSUMED"
    assert len(client.sent) == 1
    assert e.manual_operator_reset()["status"] == "FAILED"   # unresolved UNKNOWN blocks reset


def test_persistence_failure_in_engine_sends_nothing(store):
    store._fault = lambda p: (_ for _ in ()).throw(psycopg.OperationalError("db down"))
    client = FakeExchange()
    out = _engine(store, client).execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())
    assert out["status"] == "REJECTED" and client.sent == []


# ------------------------------------------------ purpose policy / lock / live gate
def test_emergency_lock_blocks_entry_but_allows_exit_and_emergency_reduce(store):
    client = FakeExchange()
    e = _engine(store, client)
    e.enter_emergency_lock()
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())["status"] == "REJECTED"
    assert e.execute_ioc_order("BTCUSDT", "BUY", 0.1, 5e4, validated_order=_token())["status"] == "REJECTED"
    assert client.sent == []
    out = e.execute_reduce_only_order("BTCUSDT", "SELL", 0.1, 5e4, validated_order=_token("SELL"))
    assert out["status"] == "SUCCESS" and out["reduce_only"] is True
    assert e._emergency_reduce("BTCUSDT", "SELL", 0.1, None, "MARKET", "EMERG:t:1")["status"] == "SUCCESS"
    assert [k for k, _ in client.sent] == ["reduce", "market"]


def test_policy_rejects_before_consuming_authorization(store):
    e = _engine(store)
    e.enter_emergency_lock()
    tok = _token()
    assert e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)["status"] == "REJECTED"
    assert store.authorization_status(tok.authorization_id) is None   # nothing burned


def test_live_trading_disabled_by_default_for_non_simulated_client(store):
    class RealLike:  # no is_simulated flag => looks like a real client
        sent = 0

        def execute_market_order(self, *a, **k):
            RealLike.sent += 1
    e = ExecutionEngine(order_store=store)
    e.toobit_client = RealLike()
    out = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())
    assert out["status"] == "REJECTED" and "live trading disabled" in out["reason"] and RealLike.sent == 0
    assert e.live_trading_enabled is False


def test_engine_without_store_fails_closed():
    e = ExecutionEngine.__new__(ExecutionEngine)
    ExecutionEngine.default_store_factory = None
    e.__init__()
    e.toobit_client = FakeExchange()
    out = e.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=_token())
    assert out["status"] == "REJECTED" and e.toobit_client.sent == []


# --------------------------------------------- emergency liquidation duplicates
def _paper(position=0.5, **kw):
    from tests.support.paper_exchange import PaperExchange
    return PaperExchange(position=position, **kw)


def test_repeated_rollback_triggers_send_one_emergency_order_set(store):
    ex = _paper(0.5)
    e = _engine(store, ex)
    r1 = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    r2 = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    r3 = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r1["status"] == "ROLLBACK_COMPLETED"
    assert r2["status"] == r3["status"] == "ROLLBACK_COMPLETED" and r2["no_position_to_close"] is True
    assert [k for k, *_ in ex.sent] == ["ioc"]          # flat => no further order of any kind
    assert e.emergency_lock == EmergencyLockState.LOCKED


def test_concurrent_rollbacks_send_one_emergency_order(store):
    ex = _paper(0.5)
    e = _engine(store, ex)
    barrier, out = threading.Barrier(8), []

    def work():
        barrier.wait()
        out.append(e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)["status"])

    ts = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(ex.sent) == 1                       # exactly one emergency order reached the exchange
    assert set(out) <= {"ROLLBACK_COMPLETED", "ROLLBACK_DUPLICATE_SUPPRESSED", "ROLLBACK_INCOMPLETE_LOCKED"}
    assert ex.positions["BTCUSDT"] == 0.0


def test_emergency_unknown_blocks_second_liquidation_and_never_resends(store):
    ex = _paper(0.5)
    ex.fail_sends = TimeoutError("lost")
    e = _engine(store, ex)
    r1 = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r1["status"] == "ROLLBACK_INCOMPLETE_LOCKED" and r1["order_status"] == "SUBMISSION_UNKNOWN"
    r2 = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)   # same episode
    assert r2["status"] == "ROLLBACK_DUPLICATE_SUPPRESSED"
    assert len(ex.sent) == 1                                         # no market fallback, no resend
    assert e.emergency_lock == EmergencyLockState.LOCKED


def test_partial_unique_index_blocks_second_active_emergency_for_symbol(store):
    for aid in ("E1", "E2"):
        store.issue_authorization(aid, "EMERGENCY_REDUCE", "BTCUSDT", "SELL", "MARKET", 1, None, True)
    store.consume_persist_and_claim("E1")
    with pytest.raises(ActiveEmergencyOrderExists):
        store.consume_persist_and_claim("E2")
    assert store.authorization_status("E2") == "UNUSED"             # rolled back, not burned
    store.issue_authorization("E3", "EMERGENCY_REDUCE", "ETHUSDT", "SELL", "MARKET", 1, None, True)
    store.consume_persist_and_claim("E3")                           # other symbol unaffected


def test_emergency_rollback_ioc_unfilled_falls_back_to_single_market(store):
    ex = _paper(0.5)
    ex.ioc_fill_ratio = 0.0
    e = _engine(store, ex)
    r = e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert r["status"] == "ROLLBACK_COMPLETED" and r["market_close_executed"] is True
    assert [k for k, *_ in ex.sent] == ["ioc", "market"]
    e.trigger_emergency_rollback("BTCUSDT", "BUY", 0.5, 5e4)
    assert len(ex.sent) == 2
