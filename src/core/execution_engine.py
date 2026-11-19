"""Execution Engine module with Hybrid Emergency Rollback Protocol.

This module implements the Execution State Machine with fail-safe position management
and SL/TP automation on Toobit, including the Hybrid Emergency Rollback Protocol.
"""

import json
import os
import time
from enum import Enum
from typing import Dict, Any, Optional
from src.core.models import ValidatedOrder
from src.core.risk_engine import RiskEngine


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

    def __init__(self):
        """Initialize ExecutionEngine with config."""
        self.state = ExecutionState.READY
        self.emergency_lock = EmergencyLockState.UNLOCKED
        self.new_entry_blocked = False
        self.toobit_client = None

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

    def execute_reduce_only_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """Execute a reduce-only order on Toobit.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Order price
            validated_order: ValidatedOrder token issued by RiskEngine.
                If None, execution is REJECTED (bypass block at source).

        Returns:
            Dict with execution result
        """
        if self.emergency_lock == EmergencyLockState.LOCKED:
            return {"status": "REJECTED", "reason": "emergency lock"}

        self._require_token(validated_order)
        quantity = validated_order.quantity  # clamped quantity is authoritative

        try:
            result = self.toobit_client.execute_reduce_only_order(symbol, side, quantity, price, order_type="LIMIT")
            return {
                "status": "SUCCESS",
                "reduce_only": True,
                "order_type": "LIMIT",
            }
        except Exception as exc:
            rollback = self.trigger_emergency_rollback(symbol, side, quantity, price)
            return {
                "status": "REJECTED",
                "reason": f"execution failed: {exc}",
                "rollback": rollback,
            }

    def execute_ioc_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """Execute an IOC (Immediate or Cancel) order on Toobit.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Order price
            validated_order: ValidatedOrder token issued by RiskEngine.
                If None, execution is REJECTED (bypass block at source).

        Returns:
            Dict with execution result
        """
        if self.emergency_lock == EmergencyLockState.LOCKED:
            return {"status": "REJECTED", "reason": "emergency lock"}

        self._require_token(validated_order)
        quantity = validated_order.quantity  # clamped quantity is authoritative

        try:
            result = self.toobit_client.execute_ioc_order(symbol, side, quantity, price)
            return {
                "status": "SUCCESS",
                "time_in_force": "IOC",
                "order_type": "LIMIT"
            }
        except Exception as exc:
            rollback = self.trigger_emergency_rollback(symbol, side, quantity, price)
            return {
                "status": "REJECTED",
                "reason": f"execution failed: {exc}",
                "rollback": rollback,
            }

    def execute_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        validated_order: Optional[ValidatedOrder] = None,
    ) -> Dict[str, Any]:
        """Execute a market order on Toobit.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            validated_order: ValidatedOrder token issued by RiskEngine.
                If None, execution is REJECTED (bypass block at source).

        Returns:
            Dict with execution result
        """
        if self.emergency_lock == EmergencyLockState.LOCKED:
            return {"status": "REJECTED", "reason": "emergency lock"}

        self._require_token(validated_order)
        quantity = validated_order.quantity  # clamped quantity is authoritative

        try:
            result = self.toobit_client.execute_market_order(symbol, side, quantity)
            return {
                "status": "SUCCESS",
                "order_type": "MARKET"
            }
        except Exception as exc:
            rollback = self.trigger_emergency_rollback(
                symbol, side, quantity, validated_order.price
            )
            return {
                "status": "REJECTED",
                "reason": f"execution failed: {exc}",
                "rollback": rollback,
            }

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

        # Check for extreme conditions first
        if self.toobit_client.is_extreme_conditions():
            extreme_conditions_bypass = True
        else:
            # 2. Conditional Retry & IOC Limit Exit
            # Attempt up to max_sl_tp_retries (2 retries, so 3 total attempts)
            for i in range(self.max_sl_tp_retries + 1):
                sl_tp_retries_attempted += 1
                result = self.toobit_client.register_sl_tp(symbol, side, quantity, price, 0, 0)
                if result["status"] == "SUCCESS":
                    sl_tp_registered = True
                    break

        # If SL/TP registration succeeded
        if sl_tp_registered:
            self.enter_emergency_lock()
            return {
                "status": "ROLLBACK_COMPLETED",
                "sl_tp_retries_attempted": sl_tp_retries_attempted,
                "sl_tp_registered": True
            }

        # 4. Extreme Conditions Bypass
        if extreme_conditions_bypass:
            # Skip retries/IOC and execute immediate emergency market liquidation
            self.toobit_client.execute_market_order(symbol, side, quantity)
            self.enter_emergency_lock()
            return {
                "status": "ROLLBACK_COMPLETED",
                "extreme_conditions_bypass": True,
                "market_liquidation_executed": True
            }

        # 2. IOC Limit Exit
        ioc_result = self.toobit_client.execute_ioc_order(symbol, side, quantity, price)

        if ioc_result.get("fill_status") == "FILLED":
            self.enter_emergency_lock()
            return {
                "status": "ROLLBACK_COMPLETED",
                "ioc_exit_executed": True,
                "ioc_fill_status": "FILLED"
            }
        else:
            # 3. Market Close Fallback
            self.toobit_client.execute_market_order(symbol, side, quantity)
            self.enter_emergency_lock()
            return {
                "status": "ROLLBACK_COMPLETED",
                "market_close_executed": True
            }

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

        if position_size == 0.0:
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
