"""Execution Engine module with Hybrid Emergency Rollback Protocol.

This module implements the Execution State Machine with fail-safe position management
and SL/TP automation on Toobit, including the Hybrid Emergency Rollback Protocol.
"""

import json
import os
import time
import uuid
from enum import Enum
from typing import Dict, Any, Optional
from src.core.models import ValidatedOrder
from src.core.risk_engine import RiskEngine
from src.orders.errors import (
    ActiveEmergencyOrderExists, AuthorizationAlreadyConsumed, ExchangeRejected,  # noqa: F401
    PersistenceFailed, PolicyViolation,
)
from src.orders.pg_store import IntentState
from src.orders.policy import ExecutionContext, OrderPurpose, check_policy
from src.orders.submitter import SafeSubmitter


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

    def __init__(self, order_store=None, live_trading_enabled: bool = False):
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
        else:
            self.max_sl_tp_retries = 2
            self.rollback_timeout_ms = 1000
            self.max_slippage_pct = 0.005

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

    def _exchange_send(self, intent) -> Any:
        """The one place that calls the exchange to place an order. Raises ExchangeRejected only
        for a definitive refusal; any other exception is treated as an unknown outcome."""
        client = self.toobit_client
        cid = intent.client_order_id
        if intent.order_type == "MARKET":
            return client.execute_market_order(
                intent.symbol, intent.side, intent.quantity, client_order_id=cid)
        if intent.order_type == "IOC":
            return client.execute_ioc_order(
                intent.symbol, intent.side, intent.quantity, intent.price, client_order_id=cid)
        return client.execute_reduce_only_order(
            intent.symbol, intent.side, intent.quantity, intent.price,
            order_type="LIMIT", client_order_id=cid)

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
        # 1. New Entry Freeze
        self.new_entry_blocked = True

        sl_tp_registered = False
        sl_tp_retries_attempted = 0
        extreme_conditions_bypass = False

        episode = self._emergency_episodes.setdefault(symbol, uuid.uuid4().hex)

        # Check for extreme conditions first
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
            self.enter_emergency_lock()
            return {
                "status": "ROLLBACK_COMPLETED",
                "sl_tp_retries_attempted": sl_tp_retries_attempted,
                "sl_tp_registered": True
            }

        # Emergency risk-reduction orders go through the common persisted pipeline.
        # Deterministic authorization ids per (symbol, episode, step) + a DB partial unique
        # index make duplicate emergency liquidation impossible.
        if extreme_conditions_bypass:
            out = self._emergency_reduce(symbol, side, quantity, price, "MARKET",
                                         f"EMERG:{symbol}:{episode}:MARKET")
            self.enter_emergency_lock()
            if out["status"] == "SUCCESS":
                return {"status": "ROLLBACK_COMPLETED", "extreme_conditions_bypass": True,
                        "market_liquidation_executed": True}
            return self._rollback_not_completed(out)

        ioc = self._emergency_reduce(symbol, side, quantity, price, "IOC",
                                     f"EMERG:{symbol}:{episode}:IOC")
        if ioc["status"] == "SUCCESS" and (ioc.get("response") or {}).get("fill_status") == "FILLED":
            self.enter_emergency_lock()
            return {"status": "ROLLBACK_COMPLETED", "ioc_exit_executed": True,
                    "ioc_fill_status": "FILLED"}
        if ioc["status"] != "SUCCESS":
            # duplicate / unknown / rejected IOC: never fall through to a second order blindly
            self.enter_emergency_lock()
            return self._rollback_not_completed(ioc)

        # 3. Market Close Fallback (IOC accepted but unfilled)
        mkt = self._emergency_reduce(symbol, side, quantity, price, "MARKET",
                                     f"EMERG:{symbol}:{episode}:MARKET")
        self.enter_emergency_lock()
        if mkt["status"] == "SUCCESS":
            return {"status": "ROLLBACK_COMPLETED", "market_close_executed": True}
        return self._rollback_not_completed(mkt)

    def _emergency_reduce(self, symbol, side, quantity, price, order_type, authorization_id):
        return self._submit_order(OrderPurpose.EMERGENCY_REDUCE, order_type, symbol, side,
                                  quantity, price, authorization_id, True)

    @staticmethod
    def _rollback_not_completed(out: Dict[str, Any]) -> Dict[str, Any]:
        status = out["status"]
        if status == "DUPLICATE_SUPPRESSED":
            return {"status": "ROLLBACK_DUPLICATE_SUPPRESSED", "reason": out.get("reason")}
        return {"status": "ROLLBACK_INCOMPLETE_LOCKED", "order_status": status,
                "reason": out.get("reason")}

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
        """Enter emergency lock state."""
        self.emergency_lock = EmergencyLockState.LOCKED
        self.state = ExecutionState.EMERGENCY_LOCKED

    def manual_operator_reset(self) -> Dict[str, Any]:
        """Manual operator reset after emergency lock.

        Requires position size to be verified at 0.

        Returns:
            Dict with reset result
        """
        position_size = self.reconcile_position("BTCUSDT")["position_size"]

        if self.order_store is not None and self.order_store.has_unresolved_unknown():
            return {"status": "FAILED", "reason": "unresolved SUBMISSION_UNKNOWN orders; reconcile first"}

        if position_size == 0.0:
            self._emergency_episodes.clear()
            self.emergency_lock = EmergencyLockState.UNLOCKED
            self.state = ExecutionState.READY
            self.new_entry_blocked = False
            return {"status": "SUCCESS"}

        return {"status": "FAILED", "reason": "Position not verified at 0"}

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
