"""Safe order submission + reconciliation on top of OrderStore (Track A).

Rules enforced here:
  * The intent is durably committed (OrderStore.consume_and_persist_intent) and then
    claimed (INTENT_PERSISTED -> SUBMITTING, committed) BEFORE the exchange is touched.
    If either DB step fails, the exchange client is never called.
  * Only a DEFINITIVE exchange rejection (ExchangeRejected) yields REJECTED. Timeouts,
    connection errors, any other exception and malformed/empty responses yield
    SUBMISSION_UNKNOWN.
  * submit_intent() can only start from INTENT_PERSISTED (compare-and-set). No retry
    loop, no exception handler, no recovery path ever resubmits an UNKNOWN order.
  * reconcile() only queries. "Not found", errors, empty/incomplete/conflicting
    responses leave the intent in SUBMISSION_UNKNOWN.

The client is any object exposing:
    submit_order(client_order_id, symbol, side, quantity, price) -> dict
    query_order(client_order_id) -> dict
(Tests use mocks only; no real Toobit calls exist in this module.)
"""

from __future__ import annotations

from typing import Any, Optional

from src.core.order_store import IntentState, OrderIntent, OrderStore

ACCEPTED_STATUSES = {"NEW", "PARTIALLY_FILLED", "FILLED"}
REJECTED_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}


class ExchangeRejected(Exception):
    """The exchange explicitly and definitively refused the order (it was NOT placed)."""


def _valid_ack(resp: Any, intent: OrderIntent) -> bool:
    return (
        isinstance(resp, dict)
        and bool(resp.get("orderId"))
        and resp.get("clientOrderId") == intent.client_order_id
    )


class SafeSubmitter:
    def __init__(self, store: OrderStore, client: Any):
        self.store = store
        self.client = client

    def authorize_and_submit(self, authorization_id: str) -> OrderIntent:
        """consume authorization + persist intent (one tx), then submit once."""
        intent = self.store.consume_and_persist_intent(authorization_id)
        return self.submit_intent(intent.intent_id)

    def submit_intent(self, intent_id: int) -> OrderIntent:
        # Claim: atomic CAS, committed before network I/O. Raises IllegalTransition
        # if the intent is in any state other than INTENT_PERSISTED (incl. UNKNOWN).
        intent = self.store.transition(
            intent_id, IntentState.INTENT_PERSISTED, IntentState.SUBMITTING, note="claimed for submit")
        try:
            resp = self.client.submit_order(
                intent.client_order_id, intent.symbol, intent.side, intent.quantity, intent.price)
        except ExchangeRejected as exc:
            return self.store.transition(
                intent_id, IntentState.SUBMITTING, IntentState.REJECTED,
                note="definitive exchange rejection", last_error=str(exc))
        except Exception as exc:  # timeout / connection loss / anything ambiguous
            return self.store.transition(
                intent_id, IntentState.SUBMITTING, IntentState.SUBMISSION_UNKNOWN,
                note="submit outcome unknown", last_error=f"{type(exc).__name__}: {exc}")
        if _valid_ack(resp, intent):
            return self.store.transition(
                intent_id, IntentState.SUBMITTING, IntentState.ACCEPTED,
                note="exchange ack", exchange_order_id=str(resp["orderId"]))
        return self.store.transition(  # empty / incomplete / mismatching ack
            intent_id, IntentState.SUBMITTING, IntentState.SUBMISSION_UNKNOWN,
            note="unusable submit response", last_error=f"bad_response: {resp!r}"[:300])

    def reconcile(self, intent_id: int) -> OrderIntent:
        """Try to resolve a SUBMISSION_UNKNOWN intent using *positive* evidence only."""
        intent = self.store.get(intent_id)
        if intent.state != IntentState.SUBMISSION_UNKNOWN:
            return intent
        try:
            resp = self.client.query_order(intent.client_order_id)
        except Exception as exc:
            self.store.record_note(intent_id, f"reconcile query failed: {type(exc).__name__}")
            return intent  # stays UNKNOWN
        verdict = self._evidence(resp, intent)
        if verdict is None:
            self.store.record_note(intent_id, f"reconcile inconclusive: {resp!r}"[:300])
            return intent  # stays UNKNOWN (not-found / empty / partial / conflicting)
        new, order_id = verdict
        return self.store.transition(
            intent_id, IntentState.SUBMISSION_UNKNOWN, new,
            note="reconciled with exchange evidence", exchange_order_id=order_id)

    @staticmethod
    def _evidence(resp: Any, intent: OrderIntent) -> Optional[tuple]:
        """(new_state, exchange_order_id) only if the response is complete, self-consistent
        and matches our intent on every identifying field. Otherwise None."""
        if not isinstance(resp, dict) or not resp:
            return None
        required = ("clientOrderId", "orderId", "status", "symbol", "side")
        if any(not resp.get(k) for k in required):
            return None
        if resp["clientOrderId"] != intent.client_order_id:
            return None
        if resp["symbol"] != intent.symbol or str(resp["side"]).upper() != intent.side:
            return None
        qty = resp.get("origQty")
        if qty is not None:
            try:
                if abs(float(qty) - intent.quantity) > 1e-9:
                    return None
            except (TypeError, ValueError):
                return None
        status = str(resp["status"]).upper()
        if status in ACCEPTED_STATUSES:
            return IntentState.ACCEPTED, str(resp["orderId"])
        if status in REJECTED_STATUSES:
            return IntentState.REJECTED, str(resp["orderId"])
        return None  # unknown status string => no conclusion

    def reconcile_all(self) -> list:
        return [self.reconcile(i.intent_id) for i in self.store.list_by_state(IntentState.SUBMISSION_UNKNOWN)]
