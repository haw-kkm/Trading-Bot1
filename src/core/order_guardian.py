"""OrderGuardian module for race condition-resistant order lifecycle state machine.

This module implements the OrderGuardian with explicit transition rules to prevent
race conditions where an order fill occurs simultaneously with a cancellation request.
"""

import uuid
import time
from enum import Enum
from typing import Dict, Any, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field

from src.core.alerts import AlertSink
from src.core.models import ValidatedOrder
from src.core.risk_engine import RiskEngine

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.core.execution_engine import ExecutionEngine


class OrderState(str, Enum):
    """Order lifecycle states."""
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    OPEN = "OPEN"
    PENDING_CANCEL = "PENDING_CANCEL"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EMERGENCY_CLOSING = "EMERGENCY_CLOSING"


class OrderEvent(str, Enum):
    """Order lifecycle events."""
    SUBMITTED = "SUBMITTED"
    OPENED = "OPENED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_CONFIRMED = "CANCEL_CONFIRMED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    RACE_CONDITION_FILL_RESOLVED = "RACE_CONDITION_FILL_RESOLVED"
    REJECTED = "REJECTED"
    EMERGENCY_CLOSING_ENTERED = "EMERGENCY_CLOSING_ENTERED"


@dataclass
class Order:
    """Represents an order with its lifecycle state."""
    order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    state: OrderState = OrderState.CREATED
    filled_quantity: float = 0.0
    fill_price: Optional[float] = None
    race_condition_resolved: bool = False
    events: List[str] = field(default_factory=list)
    rejection_reason: Optional[str] = None
    validated_order: Optional[ValidatedOrder] = None
    seen_fill_ids: set = field(default_factory=set)   # idempotency for re-delivered fills


class OrderGuardian:
    """Race condition-resistant order lifecycle state machine.

    To prevent race conditions where an order fill occurs simultaneously with a
    cancellation request, the order lifecycle state machine defines explicit
    transition rules with Fill taking absolute precedence.

    The guardian is wired to an ExecutionEngine (for real SL/TP registration on
    fill) and an AlertSink (so that any exception in an async WebSocket loop is
    surfaced instead of silently swallowed).
    """

    def __init__(
        self,
        execution_engine: Optional["ExecutionEngine"] = None,
        alert_sink: Optional[AlertSink] = None,
    ):
        """Initialize OrderGuardian.

        Args:
            execution_engine: ExecutionEngine used to register SL/TP on fill.
            alert_sink: AlertSink used to record anomalies.
        """
        self.orders: Dict[str, Order] = {}
        self.volatility_threshold = 0.03  # 3% volatility threshold
        self.volatility: Dict[str, float] = {}
        self.execution_engine = execution_engine
        self.alert_sink = alert_sink if alert_sink is not None else AlertSink()

    def create_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        validated_order: Optional["ValidatedOrder"] = None,
    ) -> Dict[str, Any]:
        """Create a new order in CREATED state.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Order price
            validated_order: ValidatedOrder token issued by RiskEngine.
                If provided, the order is bound to this authorization token.

        Returns:
            Dict with order details
        """
        try:
            if validated_order is not None:
                if not RiskEngine.verify_token(validated_order):
                    self.alert_sink.alert("ERROR", "create_order: tampered ValidatedOrder", "OrderGuardian")
                    raise ValueError("invalid ValidatedOrder token")
                # Clamped values from RiskEngine are authoritative.
                symbol = validated_order.symbol
                side = validated_order.side
                quantity = validated_order.quantity
                if validated_order.price is not None:
                    price = validated_order.price
            order_id = str(uuid.uuid4())
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price
            )
            self.orders[order_id] = order

            # Store the validated_order token on the order for downstream use
            if validated_order is not None:
                order.validated_order = validated_order

            return {
                "order_id": order_id,
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "state": order.state.value
            }
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"create_order failed: {exc}", "OrderGuardian")
            raise

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order details by order_id."""
        try:
            order = self.orders.get(order_id)
            if not order:
                return None

            return {
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "price": order.price,
                "state": order.state.value,
                "filled_quantity": order.filled_quantity,
                "fill_price": order.fill_price,
                "race_condition_resolved": order.race_condition_resolved,
                "events": order.events,
                "rejection_reason": order.rejection_reason
            }
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"get_order failed: {exc}", "OrderGuardian")
            raise

    def submit_order(self, order_id: str) -> Dict[str, Any]:
        """Submit order: CREATED -> SUBMITTED.

        Returns:
            Dict with status and reason
        """
        try:
            order = self.orders.get(order_id)
            if not order:
                return {"status": "REJECTED", "reason": "Order not found"}

            # Check volatility circuit breaker
            if self.volatility.get(order.symbol, 0.0) > self.volatility_threshold:
                return {
                    "status": "REJECTED",
                    "reason": f"Volatility circuit breaker triggered: {self.volatility[order.symbol]:.2%} > {self.volatility_threshold:.2%}"
                }

            if order.state == OrderState.CREATED:
                order.state = OrderState.SUBMITTED
                order.events.append(OrderEvent.SUBMITTED.value)
                return {"status": "SUBMITTED", "reason": "Order submitted successfully"}

            return {"status": "REJECTED", "reason": f"Cannot submit order in state {order.state.value}"}
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"submit_order failed: {exc}", "OrderGuardian")
            raise

    def open_order(self, order_id: str) -> Dict[str, Any]:
        """Open order: SUBMITTED -> OPEN.

        Returns:
            Dict with status and reason
        """
        try:
            order = self.orders.get(order_id)
            if not order:
                return {"status": "REJECTED", "reason": "Order not found"}

            if order.state == OrderState.SUBMITTED:
                order.state = OrderState.OPEN
                order.events.append(OrderEvent.OPENED.value)
                return {"status": "OPEN", "reason": "Order opened successfully"}

            return {"status": "REJECTED", "reason": f"Cannot open order in state {order.state.value}"}
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"open_order failed: {exc}", "OrderGuardian")
            raise

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel order: OPEN -> PENDING_CANCEL.

        Returns:
            Dict with status and reason
        """
        try:
            order = self.orders.get(order_id)
            if not order:
                return {"status": "REJECTED", "reason": "Order not found"}

            if order.state == OrderState.OPEN:
                order.state = OrderState.PENDING_CANCEL
                order.events.append(OrderEvent.CANCEL_REQUESTED.value)
                return {"status": "PENDING_CANCEL", "reason": "Cancel request sent"}

            # If already in PENDING_CANCEL, ignore (fill takes precedence)
            if order.state == OrderState.PENDING_CANCEL:
                return {"status": "PENDING_CANCEL", "reason": "Cancel already requested"}

            return {"status": "REJECTED", "reason": f"Cannot cancel order in state {order.state.value}"}
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"cancel_order failed: {exc}", "OrderGuardian")
            raise

    def confirm_cancel(self, order_id: str) -> Dict[str, Any]:
        """Confirm cancel: PENDING_CANCEL -> CANCELLED.

        Returns:
            Dict with status and reason
        """
        order = self.orders.get(order_id)
        if not order:
            return {"status": "REJECTED", "reason": "Order not found"}

        if order.state == OrderState.PENDING_CANCEL:
            order.state = OrderState.CANCELLED
            order.events.append(OrderEvent.CANCEL_CONFIRMED.value)
            return {"status": "CANCELLED", "reason": "Cancel confirmed"}

        return {"status": "REJECTED", "reason": f"Cannot confirm cancel in state {order.state.value}"}

    def handle_fill(self, order_id: str, filled_quantity: float, fill_price: float,
                    fill_id: Optional[str] = None) -> Dict[str, Any]:
        """Handle order fill: FILLED/PARTIALLY_FILLED with race condition resolution.

        Race Resolution Rule: If order is in PENDING_CANCEL and fill arrives,
        Fill takes absolute precedence. State transitions to FILLED/PARTIALLY_FILLED,
        records RACE_CONDITION_FILL_RESOLVED, and invokes position processing.

        After the fill, this method delegates SL/TP registration to the wired
        ExecutionEngine. If registration fails (or the engine is missing), the
        order is moved to EMERGENCY_CLOSING and an alert is raised — the async
        WebSocket loop must never silently die.

        Args:
            order_id: Order ID
            filled_quantity: Quantity filled (THIS fill only, not cumulative)
            fill_price: Fill price
            fill_id: exchange fill/trade id. When given, a re-delivered fill is ignored so the
                quantity / position is never double counted. Without it, duplicates cannot be
                detected (callers on a re-delivering channel MUST pass it).

        Returns:
            Dict with fill result and position processing status
        """
        try:
            order = self.orders.get(order_id)
            if not order:
                return {"status": "REJECTED", "reason": "Order not found"}

            if fill_id is not None:
                if fill_id in order.seen_fill_ids:
                    return {"status": "DUPLICATE_FILL_IGNORED", "position_processed": False,
                            "filled_quantity": order.filled_quantity}
                order.seen_fill_ids.add(fill_id)

            # Check if this is a race condition (fill while in PENDING_CANCEL)
            race_condition = (order.state == OrderState.PENDING_CANCEL)

            # Update fill information
            order.filled_quantity += filled_quantity
            order.fill_price = fill_price

            # Determine new state
            if order.filled_quantity >= order.quantity:
                order.state = OrderState.FILLED
                order.events.append(OrderEvent.FILLED.value)
            else:
                order.state = OrderState.PARTIALLY_FILLED
                order.events.append(OrderEvent.PARTIALLY_FILLED.value)

            # Record race condition resolution if applicable
            if race_condition:
                order.race_condition_resolved = True
                order.events.append(OrderEvent.RACE_CONDITION_FILL_RESOLVED.value)

            # Position processing
            position_processed = True

            # Real SL/TP registration via ExecutionEngine (no more hardcoded True)
            sl_tp_registered = False
            if self.execution_engine is not None:
                token = order.validated_order
                stop_loss = token.stop_loss if token is not None else None
                take_profit = token.take_profit if token is not None else None
                rollback_done = False
                try:
                    sl_tp_result = self.execution_engine.register_sl_tp(
                        order.symbol,
                        order.side,
                        order.filled_quantity,
                        fill_price,
                        stop_loss,
                        take_profit,
                        validated_order=token,
                    )
                    sl_tp_registered = bool(
                        sl_tp_result.get("sl_registered") and sl_tp_result.get("tp_registered")
                    )
                    rollback_done = "rollback" in sl_tp_result or bool(
                        sl_tp_result.get("rollback_initiated")
                    )
                except Exception as exc:
                    self.alert_sink.alert(
                        "ERROR", f"register_sl_tp raised for {order_id}: {exc}", "OrderGuardian"
                    )
                if not sl_tp_registered:
                    order.state = OrderState.EMERGENCY_CLOSING
                    order.events.append(OrderEvent.EMERGENCY_CLOSING_ENTERED.value)
                    self.alert_sink.alert(
                        "ERROR",
                        f"SL/TP registration failed for {order_id} "
                        f"({order.symbol} {order.side}); order moved to EMERGENCY_CLOSING",
                        "OrderGuardian",
                    )
                    if not rollback_done:
                        self.execution_engine.trigger_emergency_rollback(
                            order.symbol, order.side, order.filled_quantity, fill_price
                        )
            else:
                self.alert_sink.alert(
                    "WARN",
                    f"handle_fill for {order_id}: no ExecutionEngine wired; "
                    "SL/TP registration skipped",
                    "OrderGuardian",
                )

            return {
                "status": "FILLED" if order.state == OrderState.FILLED else "PARTIALLY_FILLED",
                "filled_quantity": order.filled_quantity,
                "fill_price": fill_price,
                "race_condition_resolved": race_condition,
                "position_processed": position_processed,
                "sl_tp_registered": sl_tp_registered,
            }
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"handle_fill failed: {exc}", "OrderGuardian")
            raise

    def reject_order(self, order_id: str, reason: str) -> Dict[str, Any]:
        """Reject order: -> REJECTED.

        Args:
            order_id: Order ID
            reason: Rejection reason

        Returns:
            Dict with status and reason
        """
        try:
            order = self.orders.get(order_id)
            if not order:
                return {"status": "REJECTED", "reason": "Order not found"}

            order.state = OrderState.REJECTED
            order.rejection_reason = reason
            order.events.append(OrderEvent.REJECTED.value)
            return {"status": "REJECTED", "reason": reason}
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"reject_order failed: {exc}", "OrderGuardian")
            raise

    def enter_emergency_closing(self, order_id: str) -> Dict[str, Any]:
        """Enter emergency closing state.

        Args:
            order_id: Order ID

        Returns:
            Dict with status and reason
        """
        try:
            order = self.orders.get(order_id)
            if not order:
                return {"status": "REJECTED", "reason": "Order not found"}

            order.state = OrderState.EMERGENCY_CLOSING
            order.events.append(OrderEvent.EMERGENCY_CLOSING_ENTERED.value)
            return {"status": "EMERGENCY_CLOSING", "reason": "Entered emergency closing"}
        except Exception as exc:
            self.alert_sink.alert("ERROR", f"enter_emergency_closing failed: {exc}", "OrderGuardian")
            raise

    def update_volatility(self, symbol: str, volatility: float):
        """Update volatility for a symbol.

        Args:
            symbol: Trading symbol
            volatility: Current volatility level (0.0 to 1.0)
        """
        self.volatility[symbol] = volatility
