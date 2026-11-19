"""PostgreSQL OrderStore: the SINGLE persistent record of order-submission state.

Division of responsibility:
  OrderStore     (this file)  - owns all persisted submission state + atomic transitions.
  SafeSubmitter  (submitter)  - owns submission authority, UNKNOWN handling, reconcile.
  ExecutionEngine             - owns the actual exchange API calls.

Every method opens its own short transaction (BEGIN ... COMMIT). NO transaction or lock is
held while an exchange call is in flight: claim methods commit before returning.

State machine (also enforced by a DB trigger, see migrations/001_order_safety.sql):
  INTENT_PERSISTED -> SUBMIT_PENDING -> ACCEPTED | REJECTED | SUBMISSION_UNKNOWN
  SUBMISSION_UNKNOWN -> ACCEPTED | REJECTED   (only with resolution_evidence)

SUBMISSION_UNKNOWN is specific to the order *submission* step and is unrelated to any
protection-order (SL/TP) uncertainty state.
"""

from __future__ import annotations

import hashlib
import pathlib
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Iterator, List, Optional, Set

import psycopg
from psycopg import errors as pgerr
from psycopg.rows import dict_row

from src.orders.errors import (
    ActiveEmergencyOrderExists, AuthorizationAlreadyConsumed, EntriesBlocked, IllegalTransition,
    StaleClaim,
)

MIGRATIONS_DIR = pathlib.Path(__file__).parent / "migrations"

# The entry gate (advisory lock + entry_control singleton) is GLOBAL to the database: the schema
# has no account_id. Correct ONLY for a single trading account. Multi-account support needs an
# account_id column, per-account lock keys and per-account isolation tests FIRST.
SINGLE_ACCOUNT_ONLY = True


class IntentState(str, Enum):
    INTENT_PERSISTED = "INTENT_PERSISTED"
    SUBMIT_PENDING = "SUBMIT_PENDING"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


ALLOWED_TRANSITIONS: Dict[IntentState, Set[IntentState]] = {
    IntentState.INTENT_PERSISTED: {IntentState.SUBMIT_PENDING},
    IntentState.SUBMIT_PENDING: {
        IntentState.ACCEPTED, IntentState.REJECTED, IntentState.SUBMISSION_UNKNOWN},
    IntentState.SUBMISSION_UNKNOWN: {IntentState.ACCEPTED, IntentState.REJECTED},
    IntentState.ACCEPTED: set(),
    IntentState.REJECTED: set(),
}


@dataclass(frozen=True)
class OrderIntent:
    intent_id: int
    authorization_id: str
    client_order_id: str
    purpose: str
    symbol: str
    side: str
    order_type: str
    quantity: float
    price: Optional[float]
    reduce_only: bool
    state: IntentState
    claim_token: Optional[str]
    exchange_order_id: Optional[str]
    last_error: Optional[str]


def _intent(r: dict) -> OrderIntent:
    return OrderIntent(
        r["intent_id"], r["authorization_id"], r["client_order_id"], r["purpose"], r["symbol"],
        r["side"], r["order_type"], float(r["quantity"]),
        None if r["price"] is None else float(r["price"]), r["reduce_only"],
        IntentState(r["state"]), r["claim_token"], r["exchange_order_id"], r["last_error"])


def client_order_id_for(authorization_id: str) -> str:
    """Deterministic, collision-resistant, short, charset-safe id (used for identification,
    query and reconciliation; Toobit's duplicate-clientOrderId behaviour is NOT assumed)."""
    return "tb" + hashlib.sha256(authorization_id.encode("utf-8")).hexdigest()[:30]


def migrate(dsn: str) -> None:
    """Apply pending SQL migrations (ordered files; each applied once, under an advisory lock)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(7260001)")
            conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations ("
                         "version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
            done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
            for f in sorted(MIGRATIONS_DIR.glob("*.sql")):
                if f.name not in done:
                    conn.execute(f.read_text(encoding="utf-8"))
                    conn.execute("INSERT INTO schema_migrations(version) VALUES (%s)", (f.name,))


class OrderStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        # Test-only fault injection, called with a point name inside transactions.
        self._fault: Optional[Callable[[str], None]] = None

    # ---- plumbing -------------------------------------------------------
    @contextmanager
    def _tx(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row) as conn:
            with conn.transaction():  # COMMIT on success, ROLLBACK on any exception
                yield conn

    def _hit(self, point: str) -> None:
        if self._fault is not None:
            self._fault(point)

    # ---- entry gate (SINGLE_ACCOUNT_ONLY: one global advisory lock key, see migration 002) ----
    @staticmethod
    def _gate_lock(c) -> None:
        """Transaction-scoped advisory lock shared by: ->UNKNOWN, UNKNOWN->resolved, ENTRY claim,
        freeze release. Auto-released at COMMIT/ROLLBACK. Bounded wait: if the lock cannot be
        obtained (timeout/deadlock/DB error) the exception propagates => no order is submitted."""
        c.execute("SET LOCAL lock_timeout = '3s'")
        c.execute("SELECT track_a_entry_gate_lock()")

    @staticmethod
    def _entry_block_reason(c) -> Optional[str]:
        """Fresh statements; authoritative only when called AFTER _gate_lock in the same tx."""
        if c.execute("SELECT frozen FROM entry_control WHERE id=1").fetchone()["frozen"]:
            return "entries frozen"
        if c.execute("SELECT 1 FROM order_intents WHERE state='SUBMISSION_UNKNOWN' LIMIT 1").fetchone():
            return "unresolved SUBMISSION_UNKNOWN"
        return None

    def _require_entry_gate_open(self, c) -> None:
        self._gate_lock(c)
        self._hit("entry_gate_locked")
        reason = self._entry_block_reason(c)
        if reason:
            raise EntriesBlocked(reason)

    @staticmethod
    def _event(c, intent_id, frm, to, note):
        c.execute("INSERT INTO order_events(intent_id, from_state, to_state, note)"
                  " VALUES (%s,%s,%s,%s)", (intent_id, frm, to, note))

    # ---- authorizations -------------------------------------------------
    def issue_authorization(self, authorization_id: str, purpose: str, symbol: str, side: str,
                            order_type: str, quantity: float, price: Optional[float],
                            reduce_only: bool) -> bool:
        """Idempotent issue. Returns True if newly inserted. An existing row is never altered,
        so re-issuing an already consumed authorization cannot re-arm it."""
        with self._tx() as c:
            cur = c.execute(
                "INSERT INTO authorizations(authorization_id,purpose,symbol,side,order_type,"
                "quantity,price,reduce_only) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                " ON CONFLICT (authorization_id) DO NOTHING",
                (authorization_id, purpose, symbol, side, order_type, quantity, price, reduce_only))
            return cur.rowcount == 1

    def _consume_and_insert(self, c, authorization_id: str) -> dict:
        # Atomic conditional UPDATE: exactly one concurrent caller sees a returned row.
        a = c.execute(
            "UPDATE authorizations SET status='CONSUMED', consumed_at=now()"
            " WHERE authorization_id=%s AND status='UNUSED' RETURNING *",
            (authorization_id,)).fetchone()
        if a is None:
            raise AuthorizationAlreadyConsumed(authorization_id)
        self._hit("after_consume")
        try:
            with c.transaction():  # savepoint so the violation can be translated cleanly
                row = c.execute(
                    "INSERT INTO order_intents(authorization_id, client_order_id, purpose, symbol,"
                    " side, order_type, quantity, price, reduce_only, state)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'INTENT_PERSISTED') RETURNING *",
                    (authorization_id, client_order_id_for(authorization_id), a["purpose"], a["symbol"],
                     a["side"], a["order_type"], a["quantity"], a["price"],
                     a["reduce_only"])).fetchone()
        except pgerr.UniqueViolation as exc:
            if exc.diag.constraint_name == "uq_active_emergency_reduce":
                raise ActiveEmergencyOrderExists(a["symbol"]) from exc
            if exc.diag.constraint_name == "uq_intent_authorization":
                raise AuthorizationAlreadyConsumed(authorization_id) from exc
            raise  # e.g. client_order_id collision: a real error, never mistaken for a duplicate
        self._hit("after_intent_insert")
        self._event(c, row["intent_id"], None, "INTENT_PERSISTED", "created")
        return row

    def consume_and_persist_intent(self, authorization_id: str) -> OrderIntent:
        """ONE tx: consume authorization + insert intent (INTENT_PERSISTED). Failure rolls back both."""
        with self._tx() as c:
            return _intent(self._consume_and_insert(c, authorization_id))

    def consume_persist_and_claim(self, authorization_id: str) -> OrderIntent:
        """ONE tx: consume + persist intent + acquire submit authority (SUBMIT_PENDING).
        COMMITTED before this returns; the caller then calls the exchange with no DB
        transaction or lock held."""
        with self._tx() as c:
            p = c.execute("SELECT purpose FROM authorizations WHERE authorization_id=%s",
                          (authorization_id,)).fetchone()
            if p is None:
                raise AuthorizationAlreadyConsumed(authorization_id)
            if p["purpose"] == "ENTRY":
                # lock -> re-read -> decide -> consume/claim, all in this one transaction
                self._require_entry_gate_open(c)
            row = self._consume_and_insert(c, authorization_id)
            return _intent(self._claim(c, row["intent_id"]))

    def claim_for_submit(self, intent_id: int) -> OrderIntent:
        """CAS INTENT_PERSISTED -> SUBMIT_PENDING (for recovered, provably-unsent intents)."""
        with self._tx() as c:
            p = c.execute("SELECT purpose FROM order_intents WHERE intent_id=%s",
                          (intent_id,)).fetchone()
            if p is not None and p["purpose"] == "ENTRY":
                self._require_entry_gate_open(c)
            return _intent(self._claim(c, intent_id))

    def _claim(self, c, intent_id: int) -> dict:
        token = uuid.uuid4().hex
        try:
            with c.transaction():
                row = c.execute(
                    "UPDATE order_intents SET state='SUBMIT_PENDING', claim_token=%s, claimed_at=now(),"
                    " updated_at=now() WHERE intent_id=%s AND state='INTENT_PERSISTED' RETURNING *",
                    (token, intent_id)).fetchone()
        except pgerr.CheckViolation as exc:  # DB trigger backstop for any non-Python caller path
            if "ENTRY_GATE_CLOSED" in str(exc):
                raise EntriesBlocked(str(exc)) from exc
            raise
        if row is None:
            raise IllegalTransition(f"intent {intent_id} is not INTENT_PERSISTED (cannot claim)")
        self._event(c, intent_id, "INTENT_PERSISTED", "SUBMIT_PENDING", "submit authority acquired")
        return row

    # ---- outcomes -------------------------------------------------------
    def record_outcome(self, intent_id: int, claim_token: str, new: IntentState, *,
                       exchange_order_id: Optional[str] = None, last_error: Optional[str] = None,
                       note: str = "") -> OrderIntent:
        """CAS SUBMIT_PENDING -> ACCEPTED/REJECTED/SUBMISSION_UNKNOWN, only for the claim holder."""
        if new not in ALLOWED_TRANSITIONS[IntentState.SUBMIT_PENDING]:
            raise IllegalTransition(f"SUBMIT_PENDING -> {new.value}")
        with self._tx() as c:
            if new == IntentState.SUBMISSION_UNKNOWN:
                self._gate_lock(c)   # serialize with ENTRY claims; the DB trigger sets the freeze
            row = c.execute(
                "UPDATE order_intents SET state=%s, updated_at=now(),"
                " exchange_order_id=COALESCE(%s, exchange_order_id), last_error=COALESCE(%s, last_error)"
                " WHERE intent_id=%s AND state='SUBMIT_PENDING' AND claim_token=%s RETURNING *",
                (new.value, exchange_order_id, last_error, intent_id, claim_token)).fetchone()
            if row is None:
                raise StaleClaim(f"intent {intent_id}: claim no longer valid")
            self._event(c, intent_id, "SUBMIT_PENDING", new.value, note)
            return _intent(row)

    def resolve_unknown(self, intent_id: int, new: IntentState, evidence: str,
                        exchange_order_id: Optional[str] = None) -> OrderIntent:
        """SUBMISSION_UNKNOWN -> ACCEPTED/REJECTED. `evidence` is mandatory and persisted."""
        if new not in ALLOWED_TRANSITIONS[IntentState.SUBMISSION_UNKNOWN]:
            raise IllegalTransition(f"SUBMISSION_UNKNOWN -> {new.value}")
        if not evidence:
            raise IllegalTransition("resolution requires evidence")
        with self._tx() as c:
            self._gate_lock(c)       # serialize with ENTRY claims (does NOT lift the freeze)
            row = c.execute(
                "UPDATE order_intents SET state=%s, resolution_evidence=%s, updated_at=now(),"
                " exchange_order_id=COALESCE(%s, exchange_order_id)"
                " WHERE intent_id=%s AND state='SUBMISSION_UNKNOWN' RETURNING *",
                (new.value, evidence, exchange_order_id, intent_id)).fetchone()
            if row is None:
                raise IllegalTransition(f"intent {intent_id} is not SUBMISSION_UNKNOWN")
            self._event(c, intent_id, "SUBMISSION_UNKNOWN", new.value, evidence)
            return _intent(row)

    def record_note(self, intent_id: int, note: str) -> None:
        with self._tx() as c:
            self._event(c, intent_id, None, None, note)

    def recover_stale_pending(self, older_than_s: float = 0.0) -> List[OrderIntent]:
        """SUBMIT_PENDING rows whose claim is older than `older_than_s` -> SUBMISSION_UNKNOWN.
        The claim holder died or hung; the request may or may not have reached the exchange, so
        it is NEVER resent. Use a lease >= max API timeout when live processes share the DB."""
        with self._tx() as c:
            self._gate_lock(c)
            rows = c.execute(
                "UPDATE order_intents SET state='SUBMISSION_UNKNOWN', updated_at=now(),"
                " last_error='claim_abandoned_or_process_died'"
                " WHERE state='SUBMIT_PENDING' AND claimed_at <= now() - make_interval(secs => %s)"
                " RETURNING *", (older_than_s,)).fetchall()
            for r in rows:
                self._event(c, r["intent_id"], "SUBMIT_PENDING", "SUBMISSION_UNKNOWN",
                            "recovered: claim holder gone; outcome unknown")
            return [_intent(r) for r in rows]

    # ---- persistent entry freeze ----------------------------------------
    def freeze_entries(self, reason: str) -> None:
        with self._tx() as c:
            self._gate_lock(c)
            c.execute("UPDATE entry_control SET frozen=TRUE, reason=%s,"
                      " frozen_at=COALESCE(frozen_at, now()) WHERE id=1", (reason,))

    def release_entry_freeze(self) -> bool:
        """Lift the freeze ONLY if no unresolved intent exists (re-checked by the DB under the
        gate lock). Returns False if refused. Never called by resolve/reconcile paths."""
        try:
            with self._tx() as c:
                self._gate_lock(c)
                c.execute("UPDATE entry_control SET frozen=FALSE, reason=NULL, frozen_at=NULL"
                          " WHERE id=1")
            return True
        except pgerr.CheckViolation as exc:
            if "RELEASE_REFUSED" in str(exc):
                return False
            raise

    def entries_frozen(self) -> bool:
        with self._tx() as c:
            return bool(c.execute("SELECT frozen FROM entry_control WHERE id=1").fetchone()["frozen"])

    def entries_blocked(self) -> Optional[str]:
        """Unlocked read: advisory pre-send check only (authoritative check is under the lock)."""
        with self._tx() as c:
            return self._entry_block_reason(c)

    def has_unresolved_intents(self) -> bool:
        with self._tx() as c:
            return c.execute("SELECT 1 FROM order_intents WHERE state IN"
                             " ('INTENT_PERSISTED','SUBMIT_PENDING','SUBMISSION_UNKNOWN') LIMIT 1"
                             ).fetchone() is not None

    # ---- reads ----------------------------------------------------------
    def get(self, intent_id: int) -> OrderIntent:
        with self._tx() as c:
            return _intent(c.execute("SELECT * FROM order_intents WHERE intent_id=%s",
                                     (intent_id,)).fetchone())

    def get_by_authorization(self, authorization_id: str) -> Optional[OrderIntent]:
        with self._tx() as c:
            r = c.execute("SELECT * FROM order_intents WHERE authorization_id=%s",
                          (authorization_id,)).fetchone()
            return _intent(r) if r else None

    def list_by_state(self, state: IntentState) -> List[OrderIntent]:
        with self._tx() as c:
            return [_intent(r) for r in c.execute(
                "SELECT * FROM order_intents WHERE state=%s ORDER BY intent_id", (state.value,))]

    def count_intents(self, authorization_id: Optional[str] = None) -> int:
        with self._tx() as c:
            if authorization_id is None:
                return c.execute("SELECT count(*) AS n FROM order_intents").fetchone()["n"]
            return c.execute("SELECT count(*) AS n FROM order_intents WHERE authorization_id=%s",
                             (authorization_id,)).fetchone()["n"]

    def authorization_status(self, authorization_id: str) -> Optional[str]:
        with self._tx() as c:
            r = c.execute("SELECT status FROM authorizations WHERE authorization_id=%s",
                          (authorization_id,)).fetchone()
            return r["status"] if r else None

    def has_unresolved_unknown(self, symbol: Optional[str] = None) -> bool:
        with self._tx() as c:
            q = "SELECT 1 FROM order_intents WHERE state='SUBMISSION_UNKNOWN'"
            args: tuple = ()
            if symbol:
                q += " AND symbol=%s"
                args = (symbol,)
            return c.execute(q + " LIMIT 1", args).fetchone() is not None
