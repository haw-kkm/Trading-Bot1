"""Durable order-intent store (Track A safety core).

All guarantees below are enforced by SQLite itself (transactions, conditional
UPDATEs, UNIQUE constraints, CHECK constraints and triggers) so they hold
across threads, processes and restarts -- not just in application logic.

State machine (order_intents.state):

    INTENT_PERSISTED -> SUBMITTING                     (claim; committed BEFORE any network I/O)
    SUBMITTING       -> ACCEPTED | REJECTED | SUBMISSION_UNKNOWN
    SUBMISSION_UNKNOWN -> ACCEPTED | REJECTED          (ONLY with positive evidence, never automatically
                                                        from "not found"/errors/empty responses)

ACCEPTED and REJECTED are terminal. There is deliberately NO transition from
SUBMISSION_UNKNOWN back to INTENT_PERSISTED / SUBMITTING, so an unknown order
can never be re-submitted by any code path. The same rule is mirrored in a DB
trigger so even a buggy writer cannot violate it.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Iterator, List, Optional, Set


class IntentState(str, Enum):
    INTENT_PERSISTED = "INTENT_PERSISTED"
    SUBMITTING = "SUBMITTING"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


ALLOWED_TRANSITIONS: Dict[IntentState, Set[IntentState]] = {
    IntentState.INTENT_PERSISTED: {IntentState.SUBMITTING},
    IntentState.SUBMITTING: {
        IntentState.ACCEPTED, IntentState.REJECTED, IntentState.SUBMISSION_UNKNOWN,
    },
    IntentState.SUBMISSION_UNKNOWN: {IntentState.ACCEPTED, IntentState.REJECTED},
    IntentState.ACCEPTED: set(),
    IntentState.REJECTED: set(),
}


class AuthorizationAlreadyConsumed(Exception):
    """authorization_id was already consumed (or does not exist)."""


class IllegalTransition(Exception):
    """State transition not allowed, or the row was not in the expected state."""


@dataclass(frozen=True)
class OrderIntent:
    intent_id: int
    authorization_id: str
    client_order_id: str
    symbol: str
    side: str
    quantity: float
    price: Optional[float]
    state: IntentState
    exchange_order_id: Optional[str]
    last_error: Optional[str]


def _trigger_sql() -> str:
    # Mirror ALLOWED_TRANSITIONS inside the DB.
    legal = " OR ".join(
        f"(OLD.state = '{a.value}' AND NEW.state = '{b.value}')"
        for a, bs in ALLOWED_TRANSITIONS.items() for b in bs
    )
    return f"""
    CREATE TRIGGER IF NOT EXISTS trg_intent_state_machine
    BEFORE UPDATE OF state ON order_intents
    WHEN OLD.state <> NEW.state AND NOT ({legal})
    BEGIN
        SELECT RAISE(ABORT, 'illegal order_intents state transition');
    END;
    """


_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS authorizations (
    authorization_id TEXT PRIMARY KEY,
    symbol           TEXT NOT NULL,
    side             TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    quantity         REAL NOT NULL CHECK (quantity > 0),
    price            REAL,
    status           TEXT NOT NULL DEFAULT 'UNUSED' CHECK (status IN ('UNUSED','CONSUMED')),
    consumed_at      REAL
);

CREATE TABLE IF NOT EXISTS order_intents (
    intent_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    authorization_id   TEXT NOT NULL UNIQUE REFERENCES authorizations(authorization_id),
    client_order_id    TEXT NOT NULL UNIQUE,
    symbol             TEXT NOT NULL,
    side               TEXT NOT NULL,
    quantity           REAL NOT NULL,
    price              REAL,
    state              TEXT NOT NULL CHECK (state IN
        ('INTENT_PERSISTED','SUBMITTING','SUBMISSION_UNKNOWN','ACCEPTED','REJECTED')),
    exchange_order_id  TEXT,
    last_error         TEXT,
    created_at         REAL NOT NULL,
    updated_at         REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS order_events (
    event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id  INTEGER NOT NULL REFERENCES order_intents(intent_id),
    from_state TEXT,
    to_state   TEXT,
    note       TEXT,
    at         REAL NOT NULL
);
{_trigger_sql()}
"""


class OrderStore:
    """SQLite-backed store. Every call opens its own connection => safe for
    threads and multiple processes sharing one database file."""

    def __init__(self, db_path: str, busy_timeout_s: float = 30.0):
        self.db_path = db_path
        self.busy_timeout_s = busy_timeout_s
        # Test-only fault injection: called with a point name inside transactions.
        self._fault: Optional[Callable[[str], None]] = None
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
        finally:
            conn.close()

    # ---- plumbing -------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=self.busy_timeout_s, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")  # durable on commit
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE (write lock up front) ... COMMIT, or ROLLBACK on any error."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")
        finally:
            conn.close()

    def _hit(self, point: str) -> None:
        if self._fault is not None:
            self._fault(point)

    @staticmethod
    def _row(r: sqlite3.Row) -> OrderIntent:
        return OrderIntent(
            r["intent_id"], r["authorization_id"], r["client_order_id"], r["symbol"],
            r["side"], r["quantity"], r["price"], IntentState(r["state"]),
            r["exchange_order_id"], r["last_error"],
        )

    # ---- (1)(2)(3) authorization consumption + intent persistence --------
    def issue_authorization(self, authorization_id: str, symbol: str, side: str,
                            quantity: float, price: Optional[float] = None) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO authorizations(authorization_id, symbol, side, quantity, price)"
                " VALUES (?,?,?,?,?)", (authorization_id, symbol, side, quantity, price))

    def consume_and_persist_intent(self, authorization_id: str) -> OrderIntent:
        """Atomically: UNUSED->CONSUMED *and* insert the intent row, in ONE transaction.

        * Conditional UPDATE (WHERE status='UNUSED') => exactly one caller wins.
        * UNIQUE(order_intents.authorization_id) => a 2nd intent row is impossible
          even if the first guard were bypassed.
        * Any failure rolls back BOTH, leaving the authorization UNUSED and nothing
          to submit. Returned intent is already durably committed.
        """
        now = time.time()
        with self._tx() as c:
            cur = c.execute(
                "UPDATE authorizations SET status='CONSUMED', consumed_at=?"
                " WHERE authorization_id=? AND status='UNUSED'", (now, authorization_id))
            if cur.rowcount != 1:
                raise AuthorizationAlreadyConsumed(authorization_id)
            self._hit("after_consume")
            a = c.execute("SELECT * FROM authorizations WHERE authorization_id=?",
                          (authorization_id,)).fetchone()
            try:
                c.execute(
                    "INSERT INTO order_intents(authorization_id, client_order_id, symbol, side,"
                    " quantity, price, state, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (authorization_id, f"auth-{authorization_id}", a["symbol"], a["side"],
                     a["quantity"], a["price"], IntentState.INTENT_PERSISTED.value, now, now))
            except sqlite3.IntegrityError as exc:  # UNIQUE backstop
                raise AuthorizationAlreadyConsumed(authorization_id) from exc
            self._hit("after_intent_insert")
            iid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            c.execute("INSERT INTO order_events(intent_id, from_state, to_state, note, at)"
                      " VALUES (?,NULL,?,?,?)", (iid, IntentState.INTENT_PERSISTED.value, "created", now))
            row = c.execute("SELECT * FROM order_intents WHERE intent_id=?", (iid,)).fetchone()
        return self._row(row)

    # ---- (4) explicit state machine --------------------------------------
    def transition(self, intent_id: int, expected: IntentState, new: IntentState,
                   note: str = "", exchange_order_id: Optional[str] = None,
                   last_error: Optional[str] = None) -> OrderIntent:
        """Compare-and-set transition. Fails (IllegalTransition) if the transition is not in
        the machine OR the row is no longer in `expected` (lost a race)."""
        if new not in ALLOWED_TRANSITIONS[expected]:
            raise IllegalTransition(f"{expected.value} -> {new.value} not allowed")
        now = time.time()
        with self._tx() as c:
            cur = c.execute(
                "UPDATE order_intents SET state=?, updated_at=?,"
                " exchange_order_id=COALESCE(?, exchange_order_id),"
                " last_error=COALESCE(?, last_error)"
                " WHERE intent_id=? AND state=?",
                (new.value, now, exchange_order_id, last_error, intent_id, expected.value))
            if cur.rowcount != 1:
                raise IllegalTransition(f"intent {intent_id} not in state {expected.value}")
            c.execute("INSERT INTO order_events(intent_id, from_state, to_state, note, at)"
                      " VALUES (?,?,?,?,?)", (intent_id, expected.value, new.value, note, now))
            row = c.execute("SELECT * FROM order_intents WHERE intent_id=?", (intent_id,)).fetchone()
        return self._row(row)

    def record_note(self, intent_id: int, note: str) -> None:
        """Audit-only event; never changes state."""
        with self._tx() as c:
            c.execute("INSERT INTO order_events(intent_id, from_state, to_state, note, at)"
                      " VALUES (?,NULL,NULL,?,?)", (intent_id, note, time.time()))

    # ---- reads / recovery -------------------------------------------------
    def get(self, intent_id: int) -> OrderIntent:
        conn = self._connect()
        try:
            return self._row(conn.execute(
                "SELECT * FROM order_intents WHERE intent_id=?", (intent_id,)).fetchone())
        finally:
            conn.close()

    def get_by_authorization(self, authorization_id: str) -> Optional[OrderIntent]:
        conn = self._connect()
        try:
            r = conn.execute("SELECT * FROM order_intents WHERE authorization_id=?",
                             (authorization_id,)).fetchone()
            return self._row(r) if r else None
        finally:
            conn.close()

    def list_by_state(self, state: IntentState) -> List[OrderIntent]:
        conn = self._connect()
        try:
            return [self._row(r) for r in conn.execute(
                "SELECT * FROM order_intents WHERE state=? ORDER BY intent_id", (state.value,))]
        finally:
            conn.close()

    def count_intents(self, authorization_id: Optional[str] = None) -> int:
        conn = self._connect()
        try:
            if authorization_id is None:
                return conn.execute("SELECT COUNT(*) FROM order_intents").fetchone()[0]
            return conn.execute("SELECT COUNT(*) FROM order_intents WHERE authorization_id=?",
                                (authorization_id,)).fetchone()[0]
        finally:
            conn.close()

    def authorization_status(self, authorization_id: str) -> Optional[str]:
        conn = self._connect()
        try:
            r = conn.execute("SELECT status FROM authorizations WHERE authorization_id=?",
                             (authorization_id,)).fetchone()
            return r["status"] if r else None
        finally:
            conn.close()

    def recover_on_startup(self) -> List[OrderIntent]:
        """Run once at process start.

        * SUBMITTING rows: the process died after claiming but before recording an outcome.
          The request MAY have reached the exchange, so they become SUBMISSION_UNKNOWN
          (evidence: crash mid-send). Never resent.
        * Returns INTENT_PERSISTED rows (committed, provably never sent because SUBMITTING is
          committed before any I/O) so the caller can explicitly submit them. Nothing is
          submitted here.
        """
        for it in self.list_by_state(IntentState.SUBMITTING):
            self.transition(it.intent_id, IntentState.SUBMITTING, IntentState.SUBMISSION_UNKNOWN,
                            note="process restarted while SUBMITTING; outcome unknown",
                            last_error="crash_during_submit")
        return self.list_by_state(IntentState.INTENT_PERSISTED)
