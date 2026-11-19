-- Track A: local duplicate-execution safety. PostgreSQL 16.
-- NOTE: authorization one-time-use is a LOCAL guard, not exchange exactly-once.

CREATE TABLE authorizations (
    authorization_id TEXT PRIMARY KEY,
    purpose          TEXT NOT NULL CHECK (purpose IN ('ENTRY','EXIT','PROTECTION','EMERGENCY_REDUCE')),
    symbol           TEXT NOT NULL,
    side             TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    order_type       TEXT NOT NULL CHECK (order_type IN ('MARKET','LIMIT','IOC')),
    quantity         NUMERIC(28,10) NOT NULL CHECK (quantity > 0),
    price            NUMERIC(28,10),
    reduce_only      BOOLEAN NOT NULL,
    status           TEXT NOT NULL DEFAULT 'UNUSED' CHECK (status IN ('UNUSED','CONSUMED')),
    issued_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at      TIMESTAMPTZ,
    -- purpose-specific policy enforced by the DB itself
    CONSTRAINT ck_entry_not_reduce_only CHECK ((purpose = 'ENTRY') = (NOT reduce_only)),
    CONSTRAINT ck_consumed_has_time CHECK (status <> 'CONSUMED' OR consumed_at IS NOT NULL)
);

CREATE TABLE order_intents (
    intent_id            BIGSERIAL PRIMARY KEY,
    authorization_id     TEXT NOT NULL REFERENCES authorizations(authorization_id),
    client_order_id      TEXT NOT NULL,
    purpose              TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    side                 TEXT NOT NULL,
    order_type           TEXT NOT NULL,
    quantity             NUMERIC(28,10) NOT NULL,
    price                NUMERIC(28,10),
    reduce_only          BOOLEAN NOT NULL,
    state                TEXT NOT NULL CHECK (state IN
        ('INTENT_PERSISTED','SUBMIT_PENDING','SUBMISSION_UNKNOWN','ACCEPTED','REJECTED')),
    claim_token          TEXT,
    claimed_at           TIMESTAMPTZ,
    exchange_order_id    TEXT,
    last_error           TEXT,
    resolution_evidence  TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_intent_authorization UNIQUE (authorization_id),
    CONSTRAINT uq_intent_client_order_id UNIQUE (client_order_id)
);

-- At most one unresolved emergency-reduce order per symbol (duplicate emergency liquidation guard).
CREATE UNIQUE INDEX uq_active_emergency_reduce ON order_intents (symbol)
    WHERE purpose = 'EMERGENCY_REDUCE'
      AND state IN ('INTENT_PERSISTED','SUBMIT_PENDING','SUBMISSION_UNKNOWN');

CREATE TABLE order_events (
    event_id   BIGSERIAL PRIMARY KEY,
    intent_id  BIGINT NOT NULL REFERENCES order_intents(intent_id),
    from_state TEXT,
    to_state   TEXT,
    note       TEXT,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- State machine enforced in the DB. There is NO path out of SUBMISSION_UNKNOWN back to
-- INTENT_PERSISTED / SUBMIT_PENDING, so an unknown order can never be re-submitted.
CREATE FUNCTION order_intents_state_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'INTENT_PERSISTED'   AND NEW.state = 'SUBMIT_PENDING') OR
        (OLD.state = 'SUBMIT_PENDING'     AND NEW.state IN ('ACCEPTED','REJECTED','SUBMISSION_UNKNOWN')) OR
        (OLD.state = 'SUBMISSION_UNKNOWN' AND NEW.state IN ('ACCEPTED','REJECTED'))
    ) THEN
        RAISE EXCEPTION 'illegal order_intents transition % -> %', OLD.state, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    IF OLD.state = 'SUBMISSION_UNKNOWN' AND NEW.resolution_evidence IS NULL THEN
        RAISE EXCEPTION 'resolving SUBMISSION_UNKNOWN requires resolution_evidence'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_intents_state_guard
    BEFORE UPDATE OF state ON order_intents
    FOR EACH ROW EXECUTE FUNCTION order_intents_state_guard();

-- Identity/order fields are immutable once persisted.
CREATE FUNCTION order_intents_immutable() RETURNS trigger AS $$
BEGIN
    IF NEW.authorization_id <> OLD.authorization_id OR NEW.client_order_id <> OLD.client_order_id
       OR NEW.symbol <> OLD.symbol OR NEW.side <> OLD.side OR NEW.quantity <> OLD.quantity
       OR NEW.purpose <> OLD.purpose OR NEW.reduce_only <> OLD.reduce_only THEN
        RAISE EXCEPTION 'order intent identity fields are immutable' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_intents_immutable
    BEFORE UPDATE ON order_intents
    FOR EACH ROW EXECUTE FUNCTION order_intents_immutable();
