-- Week 6: completion is no longer a synchronous saga, and the outbox carries
-- every event checkout publishes, not just scans.
--
-- Run state is disposable -- every load test starts with a reset -- so it is
-- simply dropped rather than migrated.
TRUNCATE transaction_items, transactions, scan_events;
UPDATE relay_state SET published_seq = 0;

-- /complete is now one local transaction: OPEN -> COMPLETED plus an outbox
-- row. inventory-service takes the stock out when it consumes that event, so
-- there is no in-flight saga state to record or recover.
DROP INDEX idx_transactions_completing;
ALTER TABLE transactions DROP COLUMN completing_since;
ALTER TABLE transactions DROP CONSTRAINT transactions_status_check;
ALTER TABLE transactions ADD CONSTRAINT transactions_status_check
    CHECK (status IN ('OPEN', 'COMPLETED', 'CANCELLED'));

-- The transactional outbox, now general: each row is one event, written in
-- the same database transaction as the state change it describes, and
-- published by OutboxRelay -- in seq order -- to the Kafka topic it names.
-- payload is the event's JSON body; the relay wraps it in the envelope
-- (epoch, seq, event id) at publish time.
DROP TABLE scan_events;
CREATE TABLE outbox (
    seq        BIGSERIAL   PRIMARY KEY,
    topic      TEXT        NOT NULL,
    event_type TEXT        NOT NULL,
    msg_key    TEXT,
    payload    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
