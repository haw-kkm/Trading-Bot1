-- Track A (2nd safety pass): DB-enforced, persistent new-ENTRY gate.
--
-- SINGLE_ACCOUNT_ONLY: there is no account_id column anywhere in this schema, so the advisory
-- lock key below and the singleton `entry_control` row are GLOBAL for the whole database.
-- This is only correct while Phase 1 trades exactly ONE account. Multi-account support would
-- require an account_id column, per-account lock keys and per-account isolation tests first.
--
-- WHAT THE LOCK GUARANTEES: serialization of the DB-side decisions
--   (1) a transition INTO  SUBMISSION_UNKNOWN,
--   (2) a transition OUT OF SUBMISSION_UNKNOWN,
--   (3) an ENTRY order acquiring submit authority (-> SUBMIT_PENDING),
--   (4) releasing the entry freeze.
-- All four take the SAME transaction-scoped advisory lock, then re-read state with fresh
-- statements (READ COMMITTED takes a new snapshot per statement), then change state, in one tx.
--
-- WHAT IT DOES NOT GUARANTEE: the lock is released at COMMIT, before the exchange is called
-- (no DB lock/tx is held during API calls). An ENTRY that already holds SUBMIT_PENDING can
-- still be sent after another process commits SUBMISSION_UNKNOWN. A pre-send re-check only
-- narrows that window.

CREATE TABLE entry_control (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    frozen    BOOLEAN NOT NULL DEFAULT FALSE,
    reason    TEXT,
    frozen_at TIMESTAMPTZ
);
INSERT INTO entry_control (id) VALUES (1);

-- The one lock key, defined in exactly one place.
CREATE FUNCTION track_a_entry_gate_lock() RETURNS void AS $$
    SELECT pg_advisory_xact_lock(hashtext('track_a_entry_gate'));
$$ LANGUAGE sql;

CREATE FUNCTION order_intents_entry_gate() RETURNS trigger AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NEW.state = 'SUBMIT_PENDING' AND NEW.purpose = 'ENTRY' THEN
        PERFORM track_a_entry_gate_lock();
        -- separate statements => fresh snapshots after the lock was obtained
        IF (SELECT frozen FROM entry_control WHERE id = 1) THEN
            RAISE EXCEPTION 'ENTRY_GATE_CLOSED: entries are frozen' USING ERRCODE = 'check_violation';
        END IF;
        IF EXISTS (SELECT 1 FROM order_intents WHERE state = 'SUBMISSION_UNKNOWN') THEN
            RAISE EXCEPTION 'ENTRY_GATE_CLOSED: unresolved SUBMISSION_UNKNOWN exists'
                USING ERRCODE = 'check_violation';
        END IF;
    ELSIF NEW.state = 'SUBMISSION_UNKNOWN' THEN
        PERFORM track_a_entry_gate_lock();
        UPDATE entry_control SET frozen = TRUE, reason = 'SUBMISSION_UNKNOWN', frozen_at = now()
            WHERE id = 1 AND NOT frozen;
    ELSIF OLD.state = 'SUBMISSION_UNKNOWN' THEN
        PERFORM track_a_entry_gate_lock();   -- resolving does NOT lift the freeze
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_intents_entry_gate
    BEFORE UPDATE OF state ON order_intents
    FOR EACH ROW EXECUTE FUNCTION order_intents_entry_gate();

-- Lifting the freeze is only possible with no unresolved intent, decided under the same lock.
CREATE FUNCTION entry_control_release_guard() RETURNS trigger AS $$
BEGIN
    IF OLD.frozen AND NOT NEW.frozen THEN
        PERFORM track_a_entry_gate_lock();
        IF EXISTS (SELECT 1 FROM order_intents
                   WHERE state IN ('INTENT_PERSISTED','SUBMIT_PENDING','SUBMISSION_UNKNOWN')) THEN
            RAISE EXCEPTION 'RELEASE_REFUSED: unresolved order intents exist'
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_entry_control_release_guard
    BEFORE UPDATE ON entry_control
    FOR EACH ROW EXECUTE FUNCTION entry_control_release_guard();
