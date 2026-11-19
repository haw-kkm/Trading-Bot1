"""Real account equity / drawdown for RiskEngine (P0-2).

Equity comes from the exchange adapter (`get_account_equity()` -> {"equity", "as_of"}); the
response shape is an ASSUMED contract and is UNVERIFIED against Toobit. Drawdown is measured
against a persisted HIGH-WATER MARK (see migrations/003_account_equity.sql).
"""

import math
import time
from dataclasses import dataclass
from typing import Any, Callable

import psycopg


class AccountDataUnavailable(Exception):
    """Equity could not be obtained, is malformed, or is stale. Callers fail closed."""


@dataclass(frozen=True)
class AccountRisk:
    equity: float
    peak_equity: float
    drawdown_pct: float


class PgEquityStore:
    """Persists the high-water mark in PostgreSQL (one atomic statement, no read-modify-write)."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def observe(self, equity: float, as_of: float) -> float:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(
                "INSERT INTO account_equity (id, peak_equity, last_equity, last_as_of) "
                "VALUES (1, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET "
                "peak_equity = GREATEST(account_equity.peak_equity, EXCLUDED.peak_equity), "
                "last_equity = EXCLUDED.last_equity, last_as_of = EXCLUDED.last_as_of, "
                "updated_at = now() RETURNING peak_equity", (equity, equity, as_of)).fetchone()
        return float(row[0])

    def rebase(self, equity: float, as_of: float) -> None:
        """Operator action only: reset the baseline (e.g. after a withdrawal)."""
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO account_equity (id, peak_equity, last_equity, last_as_of, rebased_at) "
                "VALUES (1, %s, %s, %s, now()) ON CONFLICT (id) DO UPDATE SET peak_equity = "
                "EXCLUDED.peak_equity, last_equity = EXCLUDED.last_equity, "
                "last_as_of = EXCLUDED.last_as_of, rebased_at = now(), updated_at = now()",
                (equity, equity, as_of))


class EquityTracker:
    def __init__(self, provider: Callable[[], Any], store: PgEquityStore,
                 max_age_s: float = 30.0, clock: Callable[[], float] = time.time):
        self.provider, self.store, self.max_age_s, self.clock = provider, store, max_age_s, clock

    def _fetch(self) -> tuple:
        try:
            raw = self.provider()
            equity, as_of = float(raw["equity"]), float(raw["as_of"])
        except Exception as exc:
            raise AccountDataUnavailable(f"equity query failed/malformed: {exc!r}") from exc
        if not (math.isfinite(equity) and equity > 0 and math.isfinite(as_of)):
            raise AccountDataUnavailable(f"invalid equity data: equity={equity} as_of={as_of}")
        age = self.clock() - as_of
        if age > self.max_age_s or age < -5.0:
            raise AccountDataUnavailable(f"equity data stale/future-dated (age {age:.1f}s)")
        return equity, as_of

    def current(self) -> AccountRisk:
        equity, as_of = self._fetch()
        try:
            peak = self.store.observe(equity, as_of)
        except Exception as exc:
            raise AccountDataUnavailable(f"equity baseline not persistable: {exc!r}") from exc
        return AccountRisk(equity, peak, max(0.0, (peak - equity) / peak * 100.0))

    def rebase(self) -> None:
        equity, as_of = self._fetch()
        self.store.rebase(equity, as_of)
