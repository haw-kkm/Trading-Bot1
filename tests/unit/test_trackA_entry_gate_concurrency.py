"""Track A 2nd pass, item 3: PostgreSQL advisory-lock entry gate under REAL concurrent transactions.

Each actor uses its own connection (and processes where noted). Interleavings are forced with
threading.Event / Barrier and by observing pg_stat_activity for an advisory-lock waiter, never by
sleeping for a guessed duration. (A short poll interval is used only while WAITING for that
observable condition.)

SINGLE_ACCOUNT_ONLY: the gate lock is global; nothing here claims multi-account safety.
"""

import os
import subprocess
import sys
import threading
import time

import psycopg
import pytest

from src.core.execution_engine import ExecutionEngine
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine
from src.orders.errors import EntriesBlocked, PersistenceFailed
from src.orders.pg_store import IntentState, OrderStore
from src.orders.submitter import SafeSubmitter
from tests.support.paper_exchange import PaperExchange

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
T = 15.0


# ------------------------------------------------------------------ helpers
def _issue_entry(store, aid):
    store.issue_authorization(aid, "ENTRY", "BTCUSDT", "BUY", "MARKET", 0.1, None, False)


def _pending_exit(store, aid="X1"):
    """A reduce-only order that already holds submit authority (SUBMIT_PENDING)."""
    store.issue_authorization(aid, "EXIT", "BTCUSDT", "SELL", "MARKET", 0.1, None, True)
    return store.consume_persist_and_claim(aid)


def _ok_sender(log):
    def sender(intent):
        log.append(intent.client_order_id)
        return {"status": "SUCCESS", "orderId": "1", "clientOrderId": intent.client_order_id}
    return sender


class Runner(threading.Thread):
    def __init__(self, fn):
        super().__init__(daemon=True)
        self.fn, self.result, self.error = fn, None, None

    def run(self):
        try:
            self.result = self.fn()
        except BaseException as exc:  # noqa: BLE001
            self.error = exc


def _wait_for_advisory_waiter(dsn, timeout=T):
    """Block until some backend is waiting on an advisory lock (observable, not guessed)."""
    deadline = time.monotonic() + timeout
    with psycopg.connect(dsn, autocommit=True) as c:
        while time.monotonic() < deadline:
            n = c.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'"
                          " AND wait_event='advisory' AND datname=current_database()").fetchone()[0]
            if n:
                return True
            time.sleep(0.01)
    return False


class OpenTx:
    """Holds an UNCOMMITTED transaction on its own connection until released."""

    def __init__(self, dsn, sql, args=()):
        self.held, self.release = threading.Event(), threading.Event()
        self.error = None

        def run():
            try:
                with psycopg.connect(dsn) as conn:       # autocommit off => explicit tx
                    conn.execute(sql, args)
                    self.held.set()
                    self.release.wait(T)
                    conn.commit()
            except BaseException as exc:  # noqa: BLE001
                self.error = exc
                self.held.set()
        self.t = threading.Thread(target=run, daemon=True)
        self.t.start()
        assert self.held.wait(T) and self.error is None, self.error

    def commit(self):
        self.release.set()
        self.t.join(T)
        assert self.error is None, self.error


TO_UNKNOWN = "UPDATE order_intents SET state='SUBMISSION_UNKNOWN' WHERE intent_id=%s"
TO_ACCEPTED = ("UPDATE order_intents SET state='ACCEPTED', resolution_evidence='exchange says NEW'"
               " WHERE intent_id=%s")


# ------------------------------------------------- A: UNKNOWN in flight (uncommitted) vs new ENTRY
def test_entry_blocked_when_unknown_transition_is_uncommitted_and_lands_first(store, pg_dsn):
    pending = _pending_exit(store)
    _issue_entry(store, "E1")
    sent = []
    sub = SafeSubmitter(store, _ok_sender(sent))
    a = OpenTx(pg_dsn, TO_UNKNOWN, (pending.intent_id,))       # A: UNKNOWN written, NOT committed
    b = Runner(lambda: sub.submit("E1"))
    b.start()
    assert _wait_for_advisory_waiter(pg_dsn), "B was not serialized behind A's gate lock"
    assert b.is_alive()                                         # B is parked, has decided nothing
    a.commit()
    b.join(T)
    assert isinstance(b.error, EntriesBlocked)                  # B re-read AFTER the lock: sees freeze
    assert sent == []                                           # exchange never reached
    assert store.authorization_status("E1") == "UNUSED"         # authorization not burned
    assert store.get(pending.intent_id).state == IntentState.SUBMISSION_UNKNOWN


# ------------------------------------------------- B: authority committed, then UNKNOWN, then send
def test_KNOWN_LIMITATION_inflight_entry_is_still_sent_after_unknown_commits(store):
    """Documents what the advisory lock CANNOT do: B already holds submit authority (committed,
    lock released). A commits UNKNOWN after B's pre-send re-check and before B's exchange call.
    The in-flight ENTRY still reaches the exchange. Mitigation only; NOT solved."""
    pending = _pending_exit(store)
    _issue_entry(store, "E1")
    sent = []
    sub = SafeSubmitter(store, _ok_sender(sent))

    def a_commits_unknown_now():
        # runs inside B's thread at the exact point: authority held, re-check passed, not yet sent
        with psycopg.connect(store.dsn, autocommit=True) as c:
            c.execute(TO_UNKNOWN, (pending.intent_id,))        # independent connection, committed

    sub._after_presend_check = a_commits_unknown_now
    res = sub.submit("E1")
    assert store.get(pending.intent_id).state == IntentState.SUBMISSION_UNKNOWN
    assert sent == [res.intent.client_order_id]                 # <-- the order WAS sent anyway
    assert res.state == IntentState.ACCEPTED
    # ...but from here on, no new ENTRY can obtain authority
    _issue_entry(store, "E2")
    with pytest.raises(EntriesBlocked):
        sub.submit("E2")


def test_presend_recheck_catches_unknown_committed_before_it(store):
    """The mitigation narrows the window: UNKNOWN committed BEFORE the re-check aborts the send."""
    pending = _pending_exit(store)
    _issue_entry(store, "E1")
    sent = []
    sub = SafeSubmitter(store, _ok_sender(sent))

    store._fault = None
    hit = {"n": 0}

    def fault(point):
        hit["n"] += 1

    store._fault = fault
    # commit UNKNOWN between claim-commit and re-check by wrapping entries_blocked's first call
    orig = store.entries_blocked

    def blocked_after_unknown():
        with psycopg.connect(store.dsn, autocommit=True) as c:
            c.execute(TO_UNKNOWN, (pending.intent_id,))
        return orig()
    store.entries_blocked = blocked_after_unknown
    res = sub.submit("E1")
    assert sent == [] and res.state == IntentState.REJECTED
    assert "aborted_before_send" in res.intent.last_error


# ------------------------------------------------- C: UNKNOWN resolution vs ENTRY authority
def test_resolve_unknown_transaction_vs_entry_claim_serialize_and_freeze_persists(store, pg_dsn):
    u = _pending_exit(store, "X1")
    store.record_outcome(u.intent_id, u.claim_token, IntentState.SUBMISSION_UNKNOWN, last_error="t")
    assert store.entries_frozen()
    _issue_entry(store, "E1")
    sent = []
    sub = SafeSubmitter(store, _ok_sender(sent))
    a = OpenTx(pg_dsn, TO_ACCEPTED, (u.intent_id,))             # resolution in flight, uncommitted
    b = Runner(lambda: sub.submit("E1"))
    b.start()
    assert _wait_for_advisory_waiter(pg_dsn)
    a.commit()
    b.join(T)
    assert store.get(u.intent_id).state == IntentState.ACCEPTED  # UNKNOWN is resolved ...
    assert isinstance(b.error, EntriesBlocked) and sent == []    # ... yet ENTRY is STILL refused
    assert store.entries_frozen()                                # resolution never lifts the freeze


def test_entry_claim_holds_gate_across_recheck_and_claim(store, pg_dsn):
    """lock -> re-read -> decide -> consume+claim is one uninterrupted transaction: while B sits
    between its decision and its commit, A's UNKNOWN transition cannot slip in."""
    pending = _pending_exit(store)
    _issue_entry(store, "E1")
    in_gate, proceed = threading.Event(), threading.Event()

    def fault(point):
        if point == "entry_gate_locked":
            in_gate.set()
            assert proceed.wait(T)
    store._fault = fault
    sent = []
    sub = SafeSubmitter(store, _ok_sender(sent))
    b = Runner(lambda: sub.submit("E1"))
    b.start()
    assert in_gate.wait(T)                                       # B holds the lock mid-decision
    a = Runner(lambda: store.record_outcome(pending.intent_id, pending.claim_token,
                                            IntentState.SUBMISSION_UNKNOWN, last_error="x"))
    a.start()
    assert _wait_for_advisory_waiter(pg_dsn)                     # A is parked behind B
    assert a.is_alive() and store.get(pending.intent_id).state == IntentState.SUBMIT_PENDING
    proceed.set()
    b.join(T)
    a.join(T)
    assert b.error is None and a.error is None
    assert store.get(pending.intent_id).state == IntentState.SUBMISSION_UNKNOWN   # A applied AFTER B
    # B's decision stood (made before A). Whether B's wire send then happens before or after A's
    # commit is intentionally NOT asserted (that is the documented in-flight window); but the
    # DB-level ORDER is: B's claim event precedes A's UNKNOWN event.
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        ev = c.execute("SELECT intent_id, to_state FROM order_events WHERE to_state IN"
                       " ('SUBMIT_PENDING','SUBMISSION_UNKNOWN') AND note NOT LIKE 'submit authority%%'"
                       " OR (to_state='SUBMIT_PENDING') ORDER BY event_id").fetchall()
    states = [(i, st) for i, st in ev if i != pending.intent_id or st == "SUBMISSION_UNKNOWN"]
    b_claim = next(n for n, (i, st) in enumerate(states) if st == "SUBMIT_PENDING" and i != pending.intent_id)
    a_unknown = next(n for n, (i, st) in enumerate(states) if st == "SUBMISSION_UNKNOWN")
    assert b_claim < a_unknown
    assert b.result is not None and len(sent) <= 1


# ------------------------------------------------- D: claim_for_submit vs UNKNOWN
def test_claim_for_submit_vs_concurrent_unknown(store, pg_dsn):
    pending = _pending_exit(store)
    _issue_entry(store, "E1")
    persisted = store.consume_and_persist_intent("E1")           # ENTRY intent: INTENT_PERSISTED, unsent
    sub = SafeSubmitter(store, _ok_sender([]))
    a = OpenTx(pg_dsn, TO_UNKNOWN, (pending.intent_id,))
    b = Runner(lambda: sub.submit_persisted(persisted.intent_id))
    b.start()
    assert _wait_for_advisory_waiter(pg_dsn)
    a.commit()
    b.join(T)
    assert isinstance(b.error, EntriesBlocked)
    assert store.get(persisted.intent_id).state == IntentState.INTENT_PERSISTED   # still unsent, recoverable


def test_unknown_after_claim_for_submit_committed_first(store, pg_dsn):
    """Opposite order: B's claim commits first; A's later UNKNOWN does not retroactively undo it."""
    pending = _pending_exit(store)
    _issue_entry(store, "E1")
    persisted = store.consume_and_persist_intent("E1")
    claimed = store.claim_for_submit(persisted.intent_id)
    store.record_outcome(pending.intent_id, pending.claim_token, IntentState.SUBMISSION_UNKNOWN, last_error="x")
    assert store.get(claimed.intent_id).state == IntentState.SUBMIT_PENDING   # in-flight: unaffected


# ------------------------------------------------- E: concurrent ENTRY submissions
_WORKER = """
import sys
from src.orders.pg_store import OrderStore
from src.orders.submitter import SafeSubmitter
from src.orders.errors import EntriesBlocked
dsn, aid, log, go = sys.argv[1:5]
import os, time
while not os.path.exists(go): time.sleep(0.005)          # start barrier (file) - all processes released together
def sender(i):
    with open(log, "a") as f: f.write(i.client_order_id + "\\n")
    return {"status": "SUCCESS", "orderId": "1"}
try:
    SafeSubmitter(OrderStore(dsn), sender).submit(aid); print("SENT")
except EntriesBlocked:
    print("BLOCKED")
"""


def _spawn(dsn, aids, tmp_path):
    log, go = tmp_path / "sends.log", tmp_path / "go"
    procs = [subprocess.Popen([sys.executable, "-I", "-c", "import sys;sys.path.insert(0,%r)\n%s" % (ROOT, _WORKER),
                               dsn, a, str(log), str(go)], stdout=subprocess.PIPE, text=True, cwd=ROOT)
             for a in aids]
    go.write_text("go")
    outs = [p.communicate(timeout=60)[0].strip() for p in procs]
    lines = log.read_text().split() if log.exists() else []
    return outs, lines


def test_multi_process_concurrent_entries_all_serialize_without_loss(store, pg_dsn, tmp_path):
    aids = [f"N{i}" for i in range(6)]
    for a in aids:
        _issue_entry(store, a)
    outs, lines = _spawn(pg_dsn, aids, tmp_path)
    assert outs.count("SENT") == 6 and len(set(lines)) == 6     # distinct approvals all proceed once each
    assert store.count_intents() == 6


def test_multi_process_concurrent_entries_all_blocked_when_frozen(store, pg_dsn, tmp_path):
    aids = [f"M{i}" for i in range(6)]
    for a in aids:
        _issue_entry(store, a)
    store.freeze_entries("test")
    outs, lines = _spawn(pg_dsn, aids, tmp_path)
    assert outs.count("BLOCKED") == 6 and lines == []
    assert all(store.authorization_status(a) == "UNUSED" for a in aids)


# ------------------------------------------------- F: lock lifecycle / fail-closed
def test_lock_released_when_transaction_rolls_back(store, pg_dsn):
    with psycopg.connect(pg_dsn) as holder:
        holder.execute("SELECT track_a_entry_gate_lock()")
        with psycopg.connect(pg_dsn) as probe:
            assert probe.execute("SELECT pg_try_advisory_xact_lock(hashtext('track_a_entry_gate'))"
                                 ).fetchone()[0] is False       # held
            probe.rollback()
        holder.rollback()                                       # tx rollback
    with psycopg.connect(pg_dsn) as probe:
        assert probe.execute("SELECT pg_try_advisory_xact_lock(hashtext('track_a_entry_gate'))"
                             ).fetchone()[0] is True            # released automatically


def test_lock_released_when_store_tx_fails_midway(store, pg_dsn):
    _issue_entry(store, "E1")

    def boom(point):
        if point == "entry_gate_locked":
            raise psycopg.OperationalError("injected after lock")
    store._fault = boom
    with pytest.raises(PersistenceFailed):
        SafeSubmitter(store, _ok_sender([])).submit("E1")
    store._fault = None
    with psycopg.connect(pg_dsn) as probe:
        assert probe.execute("SELECT pg_try_advisory_xact_lock(hashtext('track_a_entry_gate'))"
                             ).fetchone()[0] is True
    assert store.authorization_status("E1") == "UNUSED"
    assert SafeSubmitter(store, _ok_sender([])).submit("E1").state == IntentState.ACCEPTED


def test_lock_wait_timeout_fails_closed_nothing_sent(store, pg_dsn):
    _issue_entry(store, "E1")
    sent = []
    holder = psycopg.connect(pg_dsn)
    try:
        holder.execute("SELECT track_a_entry_gate_lock()")      # gate lock held by someone else
        with pytest.raises(PersistenceFailed):                  # lock_timeout (3s) -> fail closed
            SafeSubmitter(store, _ok_sender(sent)).submit("E1")
    finally:
        holder.rollback()
        holder.close()
    assert sent == [] and store.authorization_status("E1") == "UNUSED"


def test_reduce_purposes_never_wait_on_the_entry_gate(store, pg_dsn):
    """EXIT/PROTECTION/EMERGENCY_REDUCE must not be blocked by a held gate lock or a freeze."""
    store.freeze_entries("emergency")
    sent = []
    holder = psycopg.connect(pg_dsn)
    try:
        holder.execute("SELECT track_a_entry_gate_lock()")      # someone holds the gate
        for aid, purpose in (("R1", "EXIT"), ("R2", "PROTECTION"), ("R3", "EMERGENCY_REDUCE")):
            store.issue_authorization(aid, purpose, "BTCUSDT", "SELL", "MARKET", 0.1, None, True)
            t0 = time.monotonic()
            res = SafeSubmitter(store, _ok_sender(sent)).submit(aid) if aid != "R3" else None
            if aid != "R3":
                assert res.state == IntentState.ACCEPTED and time.monotonic() - t0 < 2.0
        # EMERGENCY_REDUCE claim itself takes no gate lock either (its outcome write would, so
        # only the claim is exercised while the lock is held)
        claimed = store.consume_persist_and_claim("R3")
        assert claimed.state == IntentState.SUBMIT_PENDING
    finally:
        holder.rollback()
        holder.close()


# ------------------------------------------------- G: DB-level backstop (bypass attempts)
def test_raw_sql_claim_and_release_cannot_bypass_the_gate(store, pg_dsn):
    _issue_entry(store, "E1")
    it = store.consume_and_persist_intent("E1")
    store.freeze_entries("test")
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        with pytest.raises(psycopg.errors.CheckViolation, match="ENTRY_GATE_CLOSED"):
            c.execute("UPDATE order_intents SET state='SUBMIT_PENDING', claim_token='x' WHERE intent_id=%s",
                      (it.intent_id,))
        with pytest.raises(psycopg.errors.CheckViolation, match="RELEASE_REFUSED"):
            c.execute("UPDATE entry_control SET frozen=FALSE WHERE id=1")   # unresolved intent exists
    assert store.get(it.intent_id).state == IntentState.INTENT_PERSISTED
    assert store.release_entry_freeze() is False


# ------------------------------------------------- H: restart
def test_unknown_and_entry_freeze_survive_restart(store, pg_dsn):
    ex = PaperExchange(0.0)
    ex.fail_sends = TimeoutError("lost")
    e1 = ExecutionEngine(order_store=store)
    e1.toobit_client = ex
    tok = RiskEngine().validate_trade(TradeHypothesis(
        symbol="BTCUSDT", direction="BUY", quantity=0.1, leverage=10.0, price=5e4,
        stop_loss=49000.0, take_profit=51000.0))["validated_order"]
    assert e1.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok)["status"] == "SUBMISSION_UNKNOWN"
    # "restart": brand-new store object + engine
    store2 = OrderStore(pg_dsn)
    e2 = ExecutionEngine(order_store=store2)
    ex2 = PaperExchange(0.0)
    e2.toobit_client = ex2
    assert [i.state for i in store2.list_by_state(IntentState.SUBMISSION_UNKNOWN)] == [IntentState.SUBMISSION_UNKNOWN]
    assert e2.new_entry_blocked is True                          # restored from the persisted freeze
    tok2 = RiskEngine().validate_trade(TradeHypothesis(
        symbol="BTCUSDT", direction="BUY", quantity=0.1, leverage=10.0, price=5e4,
        stop_loss=49000.0, take_profit=51000.0))["validated_order"]
    assert e2.execute_market_order("BTCUSDT", "BUY", 0.1, validated_order=tok2)["status"] == "REJECTED"
    assert ex2.sent == []
    # reduce-only exits are still allowed while entries are frozen
    ex_tok = RiskEngine().validate_trade(TradeHypothesis(
        symbol="BTCUSDT", direction="SELL", quantity=0.1, leverage=10.0, price=5e4,
        stop_loss=51000.0, take_profit=49000.0))["validated_order"]
    assert e2.execute_reduce_only_order("BTCUSDT", "SELL", 0.1, 5e4, validated_order=ex_tok)["status"] == "SUCCESS"
