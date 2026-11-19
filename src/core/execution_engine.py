"""Execution Engine module with Hybrid Emergency Rollback Protocol.

This module implements the Execution State Machine with fail-safe position management
and SL/TP automation on Toobit, including the Hybrid Emergency Rollback Protocol.
"""

import json
import os
import math
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional
from src.core.models import ValidatedOrder
from src.core.alerts import AlertSink
from src.core.risk_engine import RiskEngine
from src.orders.errors import (
    ActiveEmergencyOrderExists, AuthorizationAlreadyConsumed, EntriesBlocked,
    ExchangeRejected,  # noqa: F401
    PersistenceFailed, PolicyViolation,
)
from src.orders.pg_store import IntentState
from src.orders.policy import ExecutionContext, OrderPurpose, check_policy
from src.orders.submitter import SafeSubmitter


# ---------------------------------------------------------------------------------------
# Fill-state classification.
# NOTE: every exchange response field used below (status, executedQty, clientOrderId,
# reduceOnly, complete, as_of, ...) is a MOCK/PAPER contract. The real Toobit response format
# is UNVERIFIED. An order ACK proves acceptance, never a fill.
# ---------------------------------------------------------------------------------------
class FillState(str, Enum):
    FILLED = "FILLED"                          # fully filled (status + cumulative qty agree)
    PARTIAL = "PARTIAL"                        # terminal order with 0 < executed < requested
    UNFILLED_CONFIRMED = "UNFILLED_CONFIRMED"  # terminal order with executed == 0, explicitly
    UNCLEAR = "UNCLEAR"                        # anything else: unknown / conflicting / working


@dataclass(frozen=True)
class FillAssessment:
    state: FillState
    filled_qty: Optional[float]
    reason: str


TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
_QTY_TOL = 1e-9


def classify_fill(resp: Any, client_order_id: str, requested_qty: float) -> FillAssessment:
    """Pure classification of an order-query response. Missing data is never read as a fill
    and never as 'unfilled': it is UNCLEAR. A terminal status (REJECTED/EXPIRED/CANCELED) alone
    never proves zero fill: the cumulative executed quantity must be present and is used."""
    unclear = lambda why: FillAssessment(FillState.UNCLEAR, None, why)  # noqa: E731
    if not isinstance(resp, dict) or not resp:
        return unclear("empty or non-dict order query response")
    if resp.get("clientOrderId") != client_order_id:
        return unclear("clientOrderId missing or mismatching")
    status = str(resp.get("status") or "").upper()
    if not status:
        return unclear("order status missing")
    raw = resp.get("executedQty")
    if raw is None:
        return unclear("cumulative executed quantity missing (terminal status alone proves nothing)")
    try:
        executed = float(raw)
    except (TypeError, ValueError):
        return unclear("cumulative executed quantity not numeric")
    if math.isnan(executed) or executed < 0:
        return unclear("cumulative executed quantity invalid")
    tol = max(_QTY_TOL, abs(requested_qty) * 1e-9)
    if executed > requested_qty + tol:
        return unclear("executed quantity exceeds requested quantity")
    terminal = status in TERMINAL_ORDER_STATUSES
    if executed >= requested_qty - tol:
        if status == "FILLED":
            return FillAssessment(FillState.FILLED, executed, "status and cumulative quantity agree")
        return unclear(f"full quantity executed but status is {status}")
    if status == "FILLED":
        return unclear("status FILLED but cumulative quantity is short")
    if executed > tol:
        if terminal:
            return FillAssessment(FillState.PARTIAL, executed, f"terminal {status} after partial fill")
        return unclear(f"partial fill on non-terminal order ({status}); may still change")
    if terminal:
        return FillAssessment(FillState.UNFILLED_CONFIRMED, 0.0, f"terminal {status}, executed 0")
    return unclear(f"zero fill so far but order not terminal ({status})")


class SnapshotIncomplete(Exception):
    """An account/exchange query was not implemented, failed, was stale or incomplete.
    This means 'cannot verify', NOT 'a position/order exists'."""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind  # NOT_IMPLEMENTED | INCOMPLETE | MODE_UNSUPPORTED


class UnauthorizedExecutionError(Exception):
    """Raised when an execute_* call lacks a valid RiskEngine ValidatedOrder."""


class ExecutionState(str, Enum):
    """Execution engine states."""
    READY = "READY"
    EMERGENCY_LOCKED = "EMERGENCY_LOCKED"


class EmergencyLockState(str, Enum):
    """Emergency lock states."""
    UNLOCKED = "UNLOCKED"
    LOCKED = "LOCKED"


class ExecutionEngine:
    """Execution engine with Hybrid Emergency Rollback Protocol.

    When an order fills on Toobit but immediate server-side SL/TP registration fails
    or protection status cannot be verified, this engine triggers the Hybrid Emergency
    Rollback workflow to ensure fail-safe position management.
    """

    # Test/bootstrap hook only: callable returning an OrderStore for engines built without one.
    default_store_factory = None

    def __init__(self, order_store=None, live_trading_enabled: bool = False,
                 alert_sink: Optional[AlertSink] = None):
        """Initialize ExecutionEngine with config.

        live_trading_enabled stays False until live order placement is separately approved.
        """
        self.state = ExecutionState.READY
        self.emergency_lock = EmergencyLockState.UNLOCKED
        self.new_entry_blocked = False
        self.toobit_client = None
        self.live_trading_enabled = live_trading_enabled
        self.order_store = None
        self.submitter = None
        self._emergency_episodes: Dict[str, str] = {}
        self.alert_sink = alert_sink if alert_sink is not None else AlertSink()
        if order_store is None and ExecutionEngine.default_store_factory is not None:
            order_store = ExecutionEngine.default_store_factory()
        if order_store is not None:
            self.attach_order_store(order_store)

        # Load risk limits config
        self._load_config()

    def _load_config(self):
        """Load configuration from risk-limits.json."""
        config_path = "config/risk-limits.json"
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = json.load(f)
            self.max_sl_tp_retries = config.get("max_sl_tp_retries", 2)
            self.rollback_timeout_ms = config.get("rollback_timeout_ms", 1000)
            self.max_slippage_pct = config.get("max_slippage_pct", 0.005)
            self.snapshot_max_age_s = config.get("snapshot_max_age_s", 30.0)
        else:
            self.max_sl_tp_retries = 2
            self.rollback_timeout_ms = 1000
            self.max_slippage_pct = 0.005
            self.snapshot_max_age_s = 30.0

    @staticmethod
    def _require_token(validated_order: Optional[ValidatedOrder]) -> None:
        """Reject any execution that lacks a valid, untampered ValidatedOrder."""
        if not isinstance(validated_order, ValidatedOrder) or not RiskEngine.verify_token(validated_order):
            raise UnauthorizedExecutionError(
                "execution refused: missing or invalid ValidatedOrder token"
            )

    # ------------------------------------------------------------------
    # Order submission. ExecutionEngine owns ONLY the exchange API calls
    # (_exchange_send/_exchange_query). Persistence, one-time authorization,
    # submit authority and SUBMISSION_UNKNOWN handling live in OrderStore/SafeSubmitter.
    # ------------------------------------------------------------------
    def attach_order_store(self, store) -> None:
        self.order_store = store
        self.submitter = SafeSubmitter(store, self._exchange_send, self._exchange_query)
        # Restart/multi-process: the persisted freeze (UNKNOWN / emergency lock) wins over
        # in-memory defaults. If it cannot be read, fail closed.
        try:
            if store.entries_frozen():
                self.new_entry_blocked = True
        except Exception as exc:
            self.new_entry_blocked = True
            self.alert_sink.alert("ERROR", f"entry freeze unreadable, failing closed: {exc}",
                                  "ExecutionEngine")

    def _exchange_send(self, intent) -> Any:
        """The one place that calls the exchange to place an order. Raises ExchangeRejected only
        for a definitive refusal; any other exception is treated as an unknown outcome."""
        client = self.toobit_client
        cid = intent.client_order_id
        if intent.order_type == "MARKET":
            return client.execute_market_order(
                intent.symbol, intent.side, intent.quantity, client_order_id=cid,
                reduce_only=intent.reduce_only)
        if intent.order_type == "IOC":
            return client.execute_ioc_order(
                intent.symbol, intent.side, intent.quantity, intent.price, client_order_id=cid,
                reduce_only=intent.reduce_only)
        return client.execute_reduce_only_order(
            intent.symbol, intent.side, intent.quantity, intent.price,
            order_type="LIMIT", client_order_id=cid, reduce_only=True)

    def _exchange_query(self, client_order_id: str) -> Any:
        return self.toobit_client.query_order(client_order_id)

    def _live_gate(self) -> Optional[str]:
        """Live order placement stays disabled until separately approved. Simulated/mock
        clients (is_simulated=True) are allowed."""
        if self.live_trading_enabled or getattr(self.toobit_client, "is_simulated", False):
            return None
        return "live trading disabled (requires separate approval)"

    def _submit_order(self, purpose: OrderPurpose, order_type: str, symbol: str, side: str,
                      quantity: float, price: Optional[float], authorization_id: str,
                      token_ok: bool) -> Dict[str, Any]:
        """Common pipeline for EVERY order purpose. Returns a status dict; never raises for
        policy/persistence problems and never sends anything unless the intent was durably
        persisted and submit authority acquired first."""
        reduce_only = purpose != OrderPurpose.ENTRY
        if self.order_store is None or self.submitter is None:
            return {"status": "REJECTED", "reason": "order store not configured (fail closed)"}
        gate = self._live_gate()
        if gate:
            return {"status": "REJECTED", "reason": gate}
        ctx = ExecutionContext(self.emergency_lock == EmergencyLockState.LOCKED,
                               self.new_entry_blocked)
        try:
            check_policy(purpose, reduce_only, token_ok, ctx)
        except PolicyViolation as exc:
            return {"status": "REJECTED", "reason": str(exc)}
        try:
            self.order_store.issue_authorization(
                authorization_id, purpose.value, symbol, side, order_type, quantity, price,
                reduce_only)
            res = self.submitter.submit(authorization_id)
        except (AuthorizationAlreadyConsumed, ActiveEmergencyOrderExists) as exc:
            return {"status": "DUPLICATE_SUPPRESSED", "reason": f"duplicate order suppressed: {exc!r}"}
        except EntriesBlocked as exc:
            return {"status": "REJECTED", "reason": f"entry gate closed (DB): {exc}"}
        except PersistenceFailed as exc:
            return {"status": "REJECTED", "reason": f"persistence failed, nothing sent: {exc}"}
        except Exception as exc:  # e.g. issue_authorization DB failure: still pre-send
            return {"status": "REJECTED", "reason": f"persistence failed, nothing sent: {exc}"}
        base = {"intent_id": res.intent.intent_id, "client_order_id": res.intent.client_order_id,
                "response": res.response}
        if res.state == IntentState.ACCEPTED:
            return {"status": "SUCCESS", **base}
        if res.state == IntentState.REJECTED:
            return {"status": "REJECTED", "reason": f"exchange rejected: {res.intent.last_error}", **base}
        # SUBMISSION_UNKNOWN: do NOT roll back, do NOT resend; freeze new entries until reconciled.
        self.new_entry_blocked = True
        return {"status": "SUBMISSION_UNKNOWN",
                "reason": "submit outcome unknown; reconcile via query before any further action",
                **base}

    @staticmethod
    def _auth_id(validated_order: ValidatedOrder) -> str:
        return validated_order.authorization_id or f"sig:{validated_order.signature}"

    def execute_reduce_only_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """Reduce-only (EXIT) order. Allowed during emergency lock (risk reduction)."""
        self._require_token(validated_order)
        quantity = validated_order.quantity  # clamped quantity is authoritative
        out = self._submit_order(OrderPurpose.EXIT, "LIMIT", symbol, side, quantity, price,
                                 self._auth_id(validated_order), True)
        if out["status"] == "SUCCESS":
            out.update({"reduce_only": True, "order_type": "LIMIT"})
        return out

    def execute_ioc_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """IOC (ENTRY purpose). Refused during emergency lock / entry freeze."""
        self._require_token(validated_order)
        quantity = validated_order.quantity
        out = self._submit_order(OrderPurpose.ENTRY, "IOC", symbol, side, quantity, price,
                                 self._auth_id(validated_order), True)
        if out["status"] == "SUCCESS":
            out.update({"time_in_force": "IOC", "order_type": "LIMIT"})
        return out

    def execute_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """Market (ENTRY purpose). Refused during emergency lock / entry freeze.

        An API timeout / lost response is SUBMISSION_UNKNOWN: no rollback, no resend."""
        self._require_token(validated_order)
        quantity = validated_order.quantity
        out = self._submit_order(OrderPurpose.ENTRY, "MARKET", symbol, side, quantity,
                                 validated_order.price, self._auth_id(validated_order), True)
        if out["status"] == "SUCCESS":
            out["order_type"] = "MARKET"
        return out

    def register_sl_tp(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_loss: float,
        take_profit: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """Register SL/TP on Toobit after order fill.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Fill price
            stop_loss: Stop loss price
            take_profit: Take profit price
            validated_order: ValidatedOrder token issued by RiskEngine.
                If None, registration is REJECTED (bypass block at source).

        Returns:
            Dict with registration result
        """
        self._require_token(validated_order)
        quantity = validated_order.quantity  # clamped quantity is authoritative

        try:
            result = self.toobit_client.register_sl_tp(symbol, side, quantity, price, stop_loss, take_profit)

            if result["status"] == "SUCCESS":
                return {
                    "status": "SUCCESS",
                    "sl_registered": True,
                    "tp_registered": True
                }
            else:
                # Trigger emergency rollback on failure
                self.trigger_emergency_rollback(symbol, side, quantity, price)
                return {
                    "status": "ROLLBACK_TRIGGERED",
                    "rollback_initiated": True
                }
        except Exception as exc:
            rollback = self.trigger_emergency_rollback(symbol, side, quantity, price)
            return {
                "status": "REJECTED",
                "reason": f"SL/TP registration failed: {exc}",
                "rollback_initiated": True,
                "rollback": rollback,
            }

    def trigger_emergency_rollback(self, symbol: str, side: str, quantity: float, price: float) -> Dict[str, Any]:
        """Exception-safe wrapper: never raises; on failure freezes entries and locks."""
        self.rollback_calls = getattr(self, "rollback_calls", 0) + 1
        try:
            return self._run_emergency_rollback(symbol, side, quantity, price)
        except Exception as exc:
            self.new_entry_blocked = True
            self.enter_emergency_lock()
            return {
                "status": "ROLLBACK_FAILED_LOCKED",
                "reason": f"rollback client error: {exc}",
            }

    def _run_emergency_rollback(self, symbol: str, side: str, quantity: float, price: float) -> Dict[str, Any]:
        """Trigger Hybrid Emergency Rollback Protocol.

        When SL/TP registration fails, this protocol ensures safe position management:
        1. New Entry Freeze
        2. Conditional Retry & IOC Limit Exit
        3. Market Close Fallback
        4. Extreme Conditions Bypass
        5. Position Reconciliation
        6. Panic StateLock

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Fill price

        Returns:
            Dict with rollback result
        """
        # 1. New Entry Freeze + emergency lock from the very start (held until verified clean)
        self.new_entry_blocked = True
        self.enter_emergency_lock()

        sl_tp_registered = False
        sl_tp_retries_attempted = 0
        extreme_conditions_bypass = False

        episode = self._emergency_episodes.setdefault(symbol, uuid.uuid4().hex)

        if self.toobit_client.is_extreme_conditions():
            extreme_conditions_bypass = True
        else:
            # 2. Conditional Retry (SL/TP registration, not an order submission)
            for i in range(self.max_sl_tp_retries + 1):
                sl_tp_retries_attempted += 1
                result = self.toobit_client.register_sl_tp(symbol, side, quantity, price, 0, 0)
                if result["status"] == "SUCCESS":
                    sl_tp_registered = True
                    break

        if sl_tp_registered:
            # The position is still OPEN (merely protected): this is NOT a completed rollback.
            return {
                "status": "ROLLBACK_PROTECTED_LOCKED",
                "sl_tp_retries_attempted": sl_tp_retries_attempted,
                "sl_tp_registered": True,
                "reason": "position not closed; protected by SL/TP; emergency lock held",
            }

        stages: Dict[str, Any] = {
            "close_requested": False, "close_confirmed": False,
            "entry_cancel_requested": False, "entry_orders_cleared_confirmed": False,
            "protective_cancel_requested": False, "protective_orders_cleared_confirmed": False,
        }
        flags: Dict[str, Any] = {}
        if extreme_conditions_bypass:
            flags["extreme_conditions_bypass"] = True

        # Order size/direction come from the exchange's LATEST position, never from the
        # caller-supplied side/quantity. Unknown/unclear position => no order at all.
        try:
            pos_side, pos_qty = self._read_symbol_position(symbol)
        except SnapshotIncomplete as exc:
            return self._rollback_blocked(symbol, exc, stages)
        if pos_qty > _QTY_TOL and (abs(pos_qty - quantity) > _QTY_TOL):
            self.alert_sink.alert(
                "WARN", f"rollback {symbol}: requested qty {quantity} != actual {pos_qty}; "
                        "using the actual exchange position", "ExecutionEngine")

        if pos_qty > _QTY_TOL:
            if not extreme_conditions_bypass:
                step = self._close_step(symbol, pos_side, pos_qty, price, "IOC", episode, stages)
                if "result" in step:
                    return step["result"]
                pos_side, pos_qty = step["side"], step["qty"]
                if pos_qty <= _QTY_TOL:
                    flags["ioc_exit_executed"] = True
            if pos_qty > _QTY_TOL:
                step = self._close_step(symbol, pos_side, pos_qty, price, "MARKET", episode, stages)
                if "result" in step:
                    return step["result"]
                if step["qty"] > _QTY_TOL:
                    return self._rollback_incomplete(
                        "MARKET order acknowledged but position remains; no further orders",
                        stages, remaining=step["qty"])
                flags["market_liquidation_executed" if extreme_conditions_bypass
                      else "market_close_executed"] = True
        else:
            flags["no_position_to_close"] = True

        problem = self._verify_flat_and_clean(symbol, stages)
        if problem:
            return self._rollback_incomplete(problem, stages)
        return {"status": "ROLLBACK_COMPLETED", "stages": stages, **flags}

    # ---- rollback helpers ----------------------------------------------------------------
    def _emergency_reduce(self, symbol, side, quantity, price, order_type, authorization_id):
        return self._submit_order(OrderPurpose.EMERGENCY_REDUCE, order_type, symbol, side,
                                  quantity, price, authorization_id, True)

    def _close_step(self, symbol, pos_side, pos_qty, price, kind, episode, stages):
        """One reduce-only close order sized/directed from the actual position. Returns either
        {"result": <terminal rollback dict>} or {"side","qty"} = freshly re-read position."""
        close_side = "SELL" if pos_side == "LONG" else "BUY"
        out = self._emergency_reduce(symbol, close_side, pos_qty, price, kind,
                                     f"EMERG:{symbol}:{episode}:{kind}")
        if out["status"] == "DUPLICATE_SUPPRESSED":
            return {"result": {"status": "ROLLBACK_DUPLICATE_SUPPRESSED", "reason": out.get("reason")}}
        if out["status"] != "SUCCESS":
            return {"result": self._rollback_incomplete(
                f"{kind} order outcome {out['status']}: {out.get('reason')}", stages,
                order_status=out["status"])}
        stages["close_requested"] = True
        try:
            new_side, new_qty = self._read_symbol_position(symbol)
        except SnapshotIncomplete as exc:
            return {"result": self._rollback_blocked(symbol, exc, stages)}
        if new_qty <= _QTY_TOL:
            return {"side": None, "qty": 0.0}
        if new_side != pos_side:
            return {"result": self._rollback_incomplete(
                f"position side changed {pos_side}->{new_side} after {kind}", stages)}
        assess = self._order_actually_filled(symbol, out["client_order_id"], pos_qty,
                                             pos_before=pos_qty, pos_after=new_qty)
        stages["last_fill_assessment"] = {"order": kind, "state": assess.state.value,
                                          "filled_qty": assess.filled_qty, "reason": assess.reason}
        if kind == "IOC" and assess.state in (FillState.PARTIAL, FillState.UNFILLED_CONFIRMED):
            # Safe to continue ONLY with the verified actual remainder (re-read position).
            return {"side": new_side, "qty": new_qty}
        return {"result": self._rollback_incomplete(
            f"{kind} fill state {assess.state.value} ({assess.reason}); remaining position "
            f"{new_qty}; no further orders", stages, remaining=new_qty)}

    def _order_actually_filled(self, symbol: str, client_order_id: str, requested_qty: float,
                               pos_before: Optional[float] = None,
                               pos_after: Optional[float] = None) -> FillAssessment:
        """Authoritative fill assessment: order QUERY (not the submit ACK) + cumulative executed
        quantity + position cross-check. There is no default-to-FILLED and no default-to-unfilled:
        anything missing/failed/conflicting is UNCLEAR. fill_status/ack fields are NOT used."""
        try:
            resp = self.toobit_client.query_order(client_order_id)
        except Exception as exc:
            return FillAssessment(FillState.UNCLEAR, None, f"order query failed: {type(exc).__name__}")
        a = classify_fill(resp, client_order_id, requested_qty)
        if a.state == FillState.UNCLEAR or a.filled_qty is None:
            return a
        if pos_before is not None and pos_after is not None:
            if abs((pos_before - a.filled_qty) - pos_after) > max(_QTY_TOL, requested_qty * 1e-9):
                return FillAssessment(
                    FillState.UNCLEAR, a.filled_qty,
                    f"position mismatch: before {pos_before} - executed {a.filled_qty} != after {pos_after}")
        return a

    def _rollback_blocked(self, symbol, exc: "SnapshotIncomplete", stages) -> Dict[str, Any]:
        self.new_entry_blocked = True
        self.enter_emergency_lock()
        self.alert_sink.alert(
            "ERROR", f"emergency rollback {symbol} BLOCKED: position unknown/unclear ({exc.kind}: {exc}); "
                     "no order sent, lock held, OPERATOR ACTION REQUIRED", "ExecutionEngine")
        return {"status": "ROLLBACK_BLOCKED_POSITION_UNKNOWN", "kind": exc.kind,
                "reason": str(exc), "stages": stages}

    def _rollback_incomplete(self, reason: str, stages, **extra) -> Dict[str, Any]:
        self.new_entry_blocked = True
        self.enter_emergency_lock()
        self.alert_sink.alert("ERROR", f"emergency rollback incomplete: {reason}", "ExecutionEngine")
        return {"status": "ROLLBACK_INCOMPLETE_LOCKED", "reason": reason, "stages": stages,
                "reconciliation_pending": True, **extra}

    # ---- exchange snapshots (mock/paper contract; real Toobit adapter UNVERIFIED) ---------
    def _section(self, method: str, key: str) -> list:
        fn = getattr(self.toobit_client, method, None)
        if not callable(fn):
            raise SnapshotIncomplete("NOT_IMPLEMENTED", f"exchange adapter lacks {method}() [not implemented]")
        try:
            r = fn()
        except Exception as exc:
            raise SnapshotIncomplete("INCOMPLETE", f"{method} failed: {type(exc).__name__}: {exc}")
        if not isinstance(r, dict) or r.get("complete") is not True:
            raise SnapshotIncomplete("INCOMPLETE", f"{method} result incomplete")
        as_of = r.get("as_of")
        if not isinstance(as_of, (int, float)) or isinstance(as_of, bool):
            raise SnapshotIncomplete("INCOMPLETE", f"{method} has no as_of timestamp")
        age = time.time() - as_of
        if age > self.snapshot_max_age_s or age < -5.0:
            raise SnapshotIncomplete("INCOMPLETE", f"{method} stale/invalid timestamp (age {age:.1f}s)")
        rows = r.get(key)
        if not isinstance(rows, list):
            raise SnapshotIncomplete("INCOMPLETE", f"{method} missing '{key}' list")
        return rows

    @staticmethod
    def _parse_positions(rows: list) -> list:
        out = []
        for p in rows:
            try:
                qty = float(p["quantity"])
                if not isinstance(p.get("symbol"), str) or p.get("side") not in ("LONG", "SHORT") \
                        or math.isnan(qty) or qty < 0:
                    raise ValueError
            except (KeyError, TypeError, ValueError):
                raise SnapshotIncomplete("INCOMPLETE", f"malformed position row: {p!r}")
            out.append({"symbol": p["symbol"], "side": p["side"], "quantity": qty})
        return out

    def _read_symbol_position(self, symbol: str):
        """(side, qty) of the actual exchange position, after validating position mode.
        Only ONE_WAY mode is supported in Phase 1; hedge/unknown mode => SnapshotIncomplete."""
        fn = getattr(self.toobit_client, "get_position_mode", None)
        if not callable(fn):
            raise SnapshotIncomplete("NOT_IMPLEMENTED", "exchange adapter lacks get_position_mode()")
        try:
            mode = fn()
        except Exception as exc:
            raise SnapshotIncomplete("INCOMPLETE", f"get_position_mode failed: {type(exc).__name__}")
        if mode != "ONE_WAY":
            raise SnapshotIncomplete("MODE_UNSUPPORTED",
                                     f"position mode {mode!r} unclear/unsupported (only ONE_WAY)")
        rows = [r for r in self._parse_positions(self._section("get_all_positions", "positions"))
                if r["symbol"] == symbol and r["quantity"] > _QTY_TOL]
        if len(rows) > 1:
            raise SnapshotIncomplete("INCOMPLETE", f"conflicting multiple {symbol} positions in one-way mode")
        if not rows:
            return None, 0.0
        return rows[0]["side"], rows[0]["quantity"]

    def _symbol_state(self, symbol: str):
        side, qty = self._read_symbol_position(symbol)
        orders = [o for o in self._section("get_open_orders", "orders") if o.get("symbol") == symbol]
        conds = [o for o in self._section("get_conditional_orders", "orders") if o.get("symbol") == symbol]
        return qty, orders, conds

    def _verify_flat_and_clean(self, symbol: str, stages) -> Optional[str]:
        """Rollback completes only if ALL hold, each CONFIRMED by a fresh complete query:
        (1) position == 0, (2) no entry-capable open order, (3) no SL/TP/conditional order.
        Cancel REQUESTS are tracked separately from cancel CONFIRMATION. Returns a problem
        string, or None when everything is confirmed."""
        try:
            qty, orders, conds = self._symbol_state(symbol)
        except SnapshotIncomplete as exc:
            return f"verification incomplete ({exc.kind}): {exc}"
        entry_orders = [o for o in orders if o.get("reduceOnly") is not True]  # unknown => entry-capable
        if entry_orders or conds:
            for o in entry_orders:
                stages["entry_cancel_requested"] = True
                try:
                    self.toobit_client.cancel_order(o.get("orderId"))
                except Exception as exc:
                    self.alert_sink.alert("WARN", f"cancel_order failed: {type(exc).__name__}", "ExecutionEngine")
            for o in conds:
                stages["protective_cancel_requested"] = True
                try:
                    self.toobit_client.cancel_conditional_order(o.get("orderId"))
                except Exception as exc:
                    self.alert_sink.alert("WARN", f"cancel_conditional_order failed: {type(exc).__name__}",
                                          "ExecutionEngine")
            try:   # cancel REQUEST is not cancel CONFIRMATION: re-query everything
                qty, orders, conds = self._symbol_state(symbol)
            except SnapshotIncomplete as exc:
                return f"post-cancel verification incomplete ({exc.kind}): {exc}"
            entry_orders = [o for o in orders if o.get("reduceOnly") is not True]
        if qty > _QTY_TOL:
            return f"position {qty} present (re-opened by fill/cancel race or never closed)"
        stages["close_confirmed"] = True
        if entry_orders:
            return f"{len(entry_orders)} entry-capable open order(s) not confirmed cancelled"
        stages["entry_orders_cleared_confirmed"] = True
        if conds:
            return f"{len(conds)} SL/TP/conditional order(s) not confirmed cleared"
        stages["protective_orders_cleared_confirmed"] = True
        return None

    def create_new_entry_order(self, symbol: str, side: str, quantity: float, price: float) -> Dict[str, Any]:
        """Create a new entry order (blocked during emergency rollback).

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Order price

        Returns:
            Dict with order creation result
        """
        if self.new_entry_blocked:
            return {
                "status": "REJECTED",
                "reason": "new entry freeze"
            }

        return {"status": "SUCCESS"}

    def reconcile_position(self, symbol: str) -> Dict[str, Any]:
        """Reconcile actual open positions with exchange.

        Args:
            symbol: Trading symbol

        Returns:
            Dict with reconciliation result
        """
        position_data = self.toobit_client.get_position(symbol)
        return {
            "status": "SUCCESS",
            "position_size": position_data.get("position_size", 0.0)
        }

    def enter_emergency_lock(self):
        """Enter emergency lock state (in memory AND, best effort, persisted as an entry freeze
        so other processes / restarts also refuse new ENTRY orders)."""
        self.emergency_lock = EmergencyLockState.LOCKED
        self.state = ExecutionState.EMERGENCY_LOCKED
        if self.order_store is not None:
            try:
                self.order_store.freeze_entries("EMERGENCY_LOCK")
            except Exception as exc:  # the in-memory lock still holds; surface it
                self.alert_sink.alert("ERROR", f"could not persist entry freeze: {exc}",
                                      "ExecutionEngine")

    def manual_operator_reset(self) -> Dict[str, Any]:
        """Manual operator reset after emergency lock / SUBMISSION_UNKNOWN freeze.

        Approves the reset ONLY when ALL of the following were verified from complete, fresh
        queries over the WHOLE account (not a single symbol):
          * no unresolved order intent in PostgreSQL (UNKNOWN / pending / unsent),
          * all futures positions are zero,
          * no open (regular) orders - entry-capable orders included,
          * no conditional / SL-TP orders (cleanup policy undefined => never auto-cancelled here).

        FAILED never means "a position exists" unless kind == EXPOSURE_PRESENT. Kinds:
          NOT_IMPLEMENTED / INCOMPLETE_VERIFICATION  -> could not verify; reset NOT approved
          UNRESOLVED_INTENTS / EXPOSURE_PRESENT / OPEN_ORDERS_PRESENT / CONDITIONAL_ORDERS_PRESENT
        This is the ONLY place that lifts new_entry_blocked / the persisted entry freeze;
        reconcile()/resolve_unknown() never do (UNKNOWN resolution is necessary, not sufficient).
        """
        def failed(kind: str, reason: str, **extra) -> Dict[str, Any]:
            unverified = kind in ("NOT_IMPLEMENTED", "INCOMPLETE_VERIFICATION")
            self.alert_sink.alert("WARN", f"manual_operator_reset refused [{kind}]: {reason}",
                                  "ExecutionEngine")
            return {"status": "FAILED", "kind": kind, "verification_incomplete": unverified,
                    "reason": reason, **extra}

        if self.order_store is None:
            return failed("INCOMPLETE_VERIFICATION", "no order store: cannot verify unresolved intents")
        try:
            if self.order_store.has_unresolved_intents():
                return failed("UNRESOLVED_INTENTS",
                              "unresolved order intents exist (SUBMISSION_UNKNOWN/pending); reconcile first")
        except Exception as exc:
            return failed("INCOMPLETE_VERIFICATION", f"cannot read order intents: {exc}")

        try:
            positions = self._parse_positions(self._section("get_all_positions", "positions"))
            open_orders = self._section("get_open_orders", "orders")
            conditional = self._section("get_conditional_orders", "orders")
        except SnapshotIncomplete as exc:
            kind = "NOT_IMPLEMENTED" if exc.kind == "NOT_IMPLEMENTED" else "INCOMPLETE_VERIFICATION"
            return failed(kind, f"{exc} -- reset NOT approved; this does NOT mean a position exists")

        live = [p for p in positions if p["quantity"] > _QTY_TOL]
        if live:
            return failed("EXPOSURE_PRESENT", "open futures position(s) exist",
                          symbols=sorted({p["symbol"] for p in live}))
        if open_orders:
            return failed("OPEN_ORDERS_PRESENT", f"{len(open_orders)} open order(s) exist "
                          "(may open new exposure); cancel and re-verify")
        if conditional:
            return failed("CONDITIONAL_ORDERS_PRESENT", f"{len(conditional)} conditional/SL-TP order(s) "
                          "exist; separate cleanup policy required (not auto-cancelled)")
        try:
            released = self.order_store.release_entry_freeze()   # DB re-checks under the gate lock
        except Exception as exc:
            return failed("INCOMPLETE_VERIFICATION", f"cannot release persisted entry freeze: {exc}")
        if not released:
            return failed("UNRESOLVED_INTENTS", "an unresolved intent appeared during reset")

        self._emergency_episodes.clear()
        self.emergency_lock = EmergencyLockState.UNLOCKED
        self.state = ExecutionState.READY
        self.new_entry_blocked = False
        # Account drawdown/risk-limit re-validation needs account-equity data this adapter does
        # not provide: reported, not silently assumed.
        return {"status": "SUCCESS", "unverified": ["account_drawdown_limit"]}

    def create_order_on_fill(self, symbol: str, side: str, quantity: float, price: float) -> str:
        """Create order record on fill.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Fill price

        Returns:
            Order ID
        """
        return "order-123"
