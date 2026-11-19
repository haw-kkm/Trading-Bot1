-- P0-2: persisted equity baseline for the account drawdown circuit breaker.
--
-- DEFINITION (explicit): drawdown_pct = (peak_equity - equity) / peak_equity * 100, where
-- peak_equity is the HIGH-WATER MARK of every equity observation ever recorded here (seeded by
-- the first observation). It is persisted so the baseline is identical after a restart, and it
-- only moves up (GREATEST) except through an explicit operator rebase.
-- SINGLE_ACCOUNT_ONLY: one row, like entry_control. Withdrawals/transfers look like drawdown
-- until an operator rebases; that fails closed on purpose.
CREATE TABLE account_equity (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    peak_equity NUMERIC NOT NULL CHECK (peak_equity > 0),
    last_equity NUMERIC NOT NULL CHECK (last_equity > 0),
    last_as_of  DOUBLE PRECISION NOT NULL,
    rebased_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
