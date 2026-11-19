"""Purpose-specific approval policy. Every purpose still passes through the common
OrderStore/SafeSubmitter pipeline (persistence, one-time authorization, state reconcile).

  ENTRY             RiskEngine token required; never reduce-only; BLOCKED during emergency lock
                    or new-entry freeze.
  EXIT              RiskEngine token required; reduce-only; allowed during emergency lock.
  PROTECTION        system-issued; reduce-only; allowed during emergency lock.
  EMERGENCY_REDUCE  system-issued; reduce-only; allowed during emergency lock; at most one
                    unresolved per symbol (DB partial unique index).

reduce-only is enforced twice: here and by a DB CHECK constraint on `authorizations`.
Whether a position actually exists is enforced exchange-side by the reduceOnly flag; this
policy does not query positions.
"""

from dataclasses import dataclass
from enum import Enum

from src.orders.errors import PolicyViolation


class OrderPurpose(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    PROTECTION = "PROTECTION"
    EMERGENCY_REDUCE = "EMERGENCY_REDUCE"


@dataclass(frozen=True)
class ExecutionContext:
    emergency_locked: bool
    new_entry_blocked: bool


def check_policy(purpose: OrderPurpose, reduce_only: bool, token_ok: bool,
                 ctx: ExecutionContext) -> None:
    if purpose == OrderPurpose.ENTRY:
        if reduce_only:
            raise PolicyViolation("ENTRY must not be reduce-only")
        if not token_ok:
            raise PolicyViolation("ENTRY requires a valid RiskEngine token")
        if ctx.emergency_locked:
            raise PolicyViolation("emergency lock: new entries are forbidden")
        if ctx.new_entry_blocked:
            raise PolicyViolation("new entry freeze: new entries are forbidden")
        return
    if not reduce_only:
        raise PolicyViolation(f"{purpose.value} must be reduce-only")
    if purpose == OrderPurpose.EXIT and not token_ok:
        raise PolicyViolation("EXIT requires a valid RiskEngine token")
