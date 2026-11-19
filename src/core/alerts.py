"""Alert / notification sink for safety-critical events.

Every component in the trading pipeline (RiskEngine, OrderGuardian,
ExecutionEngine) records anomalies here instead of swallowing them.
A silent failure in an async WebSocket loop is the exact failure mode
this module exists to prevent: callers can inspect ``alerts`` after the
fact to prove that an exception was detected, rolled back, and reported.
"""

import datetime
from typing import Any, Dict, List


class AlertSink:
    """Collects structured alert records in memory.

    Thread-unsafe by design; it is a testable observation point, not a
    production log shipper. Tests inspect ``self.alerts`` directly.
    """

    def __init__(self) -> None:
        self.alerts: List[Dict[str, Any]] = []

    def alert(self, level: str, message: str, source: str = "unknown") -> None:
        """Record an alert.

        Args:
            level: ``"ERROR"``, ``"WARN"``, ``"INFO"`` ...
            message: Human-readable description of the event.
            source: Component that raised the alert.
        """
        self.alerts.append(
            {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "level": level,
                "source": source,
                "message": message,
            }
        )

    def clear(self) -> None:
        """Drop all recorded alerts (used between test cases)."""
        self.alerts.clear()

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"AlertSink(records={len(self.alerts)})"