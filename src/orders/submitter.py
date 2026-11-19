"""SafeSubmitter: owns submission authority and SUBMISSION_UNKNOWN handling.

It never talks to the exchange itself: it is handed `sender(intent)` / `query(client_order_id)`
callables (provided by ExecutionEngine). Guarantees:
  * Intent persisted + authorization consumed + submit authority acquired in ONE committed
    transaction BEFORE the sender is invoked; the sender runs with no DB tx/lock held.
  * If that DB step fails -> PersistenceFailed, sender never called.
  * Only ExchangeRejected => REJECTED. Any other exception or an unusable response =>
    SUBMISSION_UNKNOWN. No retry loop; no code path resends an UNKNOWN intent.
  * reconcile() only queries; not-found / errors / empty / partial / conflicting => stays UNKNOWN.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional

import psycopg

from src.orders.errors import (
    EntriesBlocked, ExchangeRejected, IllegalTransition, PersistenceFailed, StaleClaim,
)
from src.orders.pg_store import IntentState, OrderIntent, OrderStore

ACCEPTED_STATUSES = {"NEW", "PARTIALLY_FILLED", "FILLED"}
REJECTED_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}


def default_ack_validator(resp: Any, intent: OrderIntent) -> bool:
    """Acknowledged only for a non-empty dict that positively reports success / an order id and,
    when it echoes clientOrderId, echoes OURS. The exact Toobit ack format is an open API
    verification item; adjust here (single place) once verified."""
    if not isinstance(resp, dict) or not resp:
        return False
    echoed = resp.get("clientOrderId")
    if echoed is not None and echoed != intent.client_order_id:
        return False
    return bool(resp.get("orderId")) or resp.get("status") == "SUCCESS"


@dataclass(frozen=True)
class SubmissionResult:
    intent: OrderIntent
    state: IntentState
    response: Any = None
    recorded: bool = True   # False if the outcome could not be written (row stays SUBMIT_PENDING)


class SafeSubmitter:
    def __init__(self, store: OrderStore, sender: Callable[[OrderIntent], Any],
                 query: Optional[Callable[[str], Any]] = None,
                 ack_validator: Callable[[Any, OrderIntent], bool] = default_ack_validator):
        self.store = store
        self.sender = sender
        self.query = query
        self.ack_validator = ack_validator
        # Test hook: runs right after the pre-send re-check, before the exchange call.
        self._after_presend_check: Optional[Callable[[], None]] = None

    def submit(self, authorization_id: str) -> SubmissionResult:
        """Raises AuthorizationAlreadyConsumed / ActiveEmergencyOrderExists / PersistenceFailed
        BEFORE anything is sent."""
        try:
            intent = self.store.consume_persist_and_claim(authorization_id)
        except EntriesBlocked:
            raise
        except psycopg.Error as exc:
            raise PersistenceFailed(str(exc)) from exc
        return self._send_and_record(intent)

    def submit_persisted(self, intent_id: int) -> SubmissionResult:
        """Explicit submission of a recovered INTENT_PERSISTED intent (never UNKNOWN ones)."""
        try:
            intent = self.store.claim_for_submit(intent_id)
        except EntriesBlocked:
            raise
        except psycopg.Error as exc:
            raise PersistenceFailed(str(exc)) from exc
        return self._send_and_record(intent)

    def _send_and_record(self, intent: OrderIntent) -> SubmissionResult:
        # --- no DB transaction / lock is held from here until the outcome is recorded ---
        resp: Any = None
        # Pre-send re-check for ENTRY: MITIGATION ONLY. It narrows, but cannot close, the window
        # between "authority committed" and "request on the wire" (no DB lock may be held across
        # the API call). A block that commits after this point is NOT prevented.
        if intent.purpose == "ENTRY":
            try:
                reason = self.store.entries_blocked()
            except psycopg.Error as exc:
                reason = f"gate state unreadable: {type(exc).__name__}"   # fail closed
            if reason:
                try:
                    done = self.store.record_outcome(
                        intent.intent_id, intent.claim_token, IntentState.REJECTED,
                        last_error=f"aborted_before_send: {reason}", note="entry gate closed pre-send")
                    return SubmissionResult(done, done.state, None)
                except (StaleClaim, psycopg.Error):
                    return SubmissionResult(intent, IntentState.SUBMISSION_UNKNOWN, None, recorded=False)
        if self._after_presend_check is not None:
            self._after_presend_check()
        try:
            resp = self.sender(intent)
            if self.ack_validator(resp, intent):
                new, kw = IntentState.ACCEPTED, dict(
                    exchange_order_id=str(resp["orderId"]) if resp.get("orderId") else None,
                    note="exchange ack")
            else:
                new, kw = IntentState.SUBMISSION_UNKNOWN, dict(
                    last_error=f"unusable_response: {resp!r}"[:300], note="unusable submit response")
        except ExchangeRejected as exc:
            new, kw = IntentState.REJECTED, dict(
                last_error=str(exc)[:300], note="definitive exchange rejection")
        except Exception as exc:  # timeout / connection loss / anything ambiguous
            new, kw = IntentState.SUBMISSION_UNKNOWN, dict(
                last_error=f"{type(exc).__name__}: {exc}"[:300], note="submit outcome unknown")
        try:
            done = self.store.record_outcome(intent.intent_id, intent.claim_token, new, **kw)
            return SubmissionResult(done, done.state, resp)
        except (StaleClaim, psycopg.Error):
            # Outcome not recorded: row is SUBMIT_PENDING (or already recovered to UNKNOWN).
            # Either way it is treated as unknown and is never resent.
            return SubmissionResult(intent, IntentState.SUBMISSION_UNKNOWN, resp, recorded=False)

    # ---- recovery / reconcile (never sends) ------------------------------
    def recover_on_startup(self, lease_s: float = 0.0) -> List[OrderIntent]:
        """Stale SUBMIT_PENDING -> SUBMISSION_UNKNOWN. Returns recovered INTENT_PERSISTED
        intents (provably unsent) for EXPLICIT submission by the caller; submits nothing."""
        self.store.recover_stale_pending(lease_s)
        return self.store.list_by_state(IntentState.INTENT_PERSISTED)

    def reconcile(self, intent_id: int) -> OrderIntent:
        intent = self.store.get(intent_id)
        if intent.state != IntentState.SUBMISSION_UNKNOWN or self.query is None:
            return intent
        try:
            resp = self.query(intent.client_order_id)
        except Exception as exc:
            self.store.record_note(intent_id, f"reconcile query failed: {type(exc).__name__}")
            return intent
        verdict = self._evidence(resp, intent)
        if verdict is None:
            self.store.record_note(intent_id, f"reconcile inconclusive: {resp!r}"[:300])
            return intent
        new, order_id = verdict
        try:
            return self.store.resolve_unknown(
                intent_id, new, evidence=f"exchange query: {resp!r}"[:500], exchange_order_id=order_id)
        except IllegalTransition:
            return self.store.get(intent_id)

    @staticmethod
    def _evidence(resp: Any, intent: OrderIntent) -> Optional[tuple]:
        if not isinstance(resp, dict) or not resp:
            return None
        if any(not resp.get(k) for k in ("clientOrderId", "orderId", "status", "symbol", "side")):
            return None
        if resp["clientOrderId"] != intent.client_order_id or resp["symbol"] != intent.symbol \
                or str(resp["side"]).upper() != intent.side:
            return None
        if resp.get("origQty") is not None:
            try:
                if abs(float(resp["origQty"]) - intent.quantity) > 1e-9:
                    return None
            except (TypeError, ValueError):
                return None
        status = str(resp["status"]).upper()
        if status in ACCEPTED_STATUSES:
            return IntentState.ACCEPTED, str(resp["orderId"])
        if status in REJECTED_STATUSES:
            return IntentState.REJECTED, str(resp["orderId"])
        return None

    def reconcile_all(self) -> List[OrderIntent]:
        return [self.reconcile(i.intent_id)
                for i in self.store.list_by_state(IntentState.SUBMISSION_UNKNOWN)]
