"""Exceptions shared by the order-safety layer."""


class ExchangeRejected(Exception):
    """The exchange DEFINITIVELY refused the order (it was not placed).

    Anything else raised by an exchange call (timeout, connection loss, 5xx,
    malformed body, ...) is ambiguous and is treated as SUBMISSION_UNKNOWN.
    """


class AuthorizationAlreadyConsumed(Exception):
    """authorization_id was already consumed, or does not exist."""


class ActiveEmergencyOrderExists(Exception):
    """An un-resolved EMERGENCY_REDUCE intent already exists for this symbol."""


class IllegalTransition(Exception):
    """Transition not in the state machine, or the row was not in the expected state."""


class StaleClaim(Exception):
    """Submission claim no longer valid (row moved on, e.g. recovered to UNKNOWN)."""


class PersistenceFailed(Exception):
    """A DB step that must precede the exchange call failed; nothing was sent."""


class PolicyViolation(Exception):
    """Order purpose policy refused the order; nothing was persisted or sent."""


class LiveTradingDisabled(Exception):
    """Live order placement is disabled until separately approved."""


class EntriesBlocked(Exception):
    """New ENTRY submit authority refused: entries are frozen / an unresolved
    SUBMISSION_UNKNOWN exists. The authorization is NOT consumed."""
