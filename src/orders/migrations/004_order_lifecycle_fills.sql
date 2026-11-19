-- P1-1: separate the SUBMISSION result (state: ACCEPTED/REJECTED/UNKNOWN...) from the exchange
-- order LIFECYCLE (exchange_status: NEW/PARTIALLY_FILLED/FILLED/CANCELED/EXPIRED/...) and the
-- cumulative FILLED QUANTITY. A CANCELED/EXPIRED order was still ACCEPTED, and it may carry a
-- partial fill; mapping it to REJECTED lost that fill.
--
-- The exchange status vocabulary and fill-report format are an ASSUMED contract (Toobit
-- unverified); exchange_status is therefore free text, not a CHECK-constrained enum.

ALTER TABLE order_intents
    ADD COLUMN exchange_status       TEXT,
    ADD COLUMN filled_qty            NUMERIC(28,10) NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    ADD COLUMN lifecycle_updated_at  TIMESTAMPTZ;

-- One row per exchange fill; UNIQUE(intent_id, exchange_fill_id) makes re-delivery idempotent.
CREATE TABLE order_fills (
    fill_row_id      BIGSERIAL PRIMARY KEY,
    intent_id        BIGINT NOT NULL REFERENCES order_intents(intent_id),
    exchange_fill_id TEXT NOT NULL,
    qty              NUMERIC(28,10) NOT NULL CHECK (qty > 0),
    price            NUMERIC(28,10),
    at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_fill_once UNIQUE (intent_id, exchange_fill_id)
);

-- Cumulative filled quantity can only grow (a late/stale report must never shrink it), and a
-- terminal lifecycle status can never revert to a non-terminal one.
CREATE FUNCTION order_intents_lifecycle_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.filled_qty < OLD.filled_qty THEN
        RAISE EXCEPTION 'filled_qty may not decrease (% -> %)', OLD.filled_qty, NEW.filled_qty
            USING ERRCODE = 'check_violation';
    END IF;
    IF OLD.exchange_status IN ('FILLED','CANCELED','EXPIRED','REJECTED')
       AND NEW.exchange_status IS DISTINCT FROM OLD.exchange_status
       AND NEW.exchange_status IN ('NEW','PARTIALLY_FILLED') THEN
        RAISE EXCEPTION 'terminal exchange_status % may not revert to %', OLD.exchange_status,
            NEW.exchange_status USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_intents_lifecycle_guard
    BEFORE UPDATE ON order_intents
    FOR EACH ROW EXECUTE FUNCTION order_intents_lifecycle_guard();
