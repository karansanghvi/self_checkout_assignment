-- checkout_db: owned by checkout-service, and by nobody else.

-- COMPLETING is the completion saga's in-flight state: claimed, stock not yet
-- confirmed by inventory-service. It is never shown on the wire (the contract
-- enum is OPEN | COMPLETED | CANCELLED); see TransactionService#wireStatus.
CREATE TABLE transactions (
    id               UUID PRIMARY KEY,
    station_id       TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('OPEN', 'COMPLETING', 'COMPLETED', 'CANCELLED')),
    item_count       INT           NOT NULL DEFAULT 0,
    running_total    NUMERIC(12,2) NOT NULL DEFAULT 0,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completing_since TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ
);

-- RecoverySweeper's lookup. Partial, so it indexes only the (normally zero)
-- sagas in flight, and costs nothing on the hot scan path.
CREATE INDEX idx_transactions_completing ON transactions (completing_since)
    WHERE status = 'COMPLETING';

-- Aggregated per SKU rather than one row per scanned unit: this is what makes
-- receipt lines a plain SELECT, and keeps the scan path a single upsert. No
-- foreign key to items -- items live in catalog_db; the SKU was validated
-- against the catalog replica before the scan was accepted.
CREATE TABLE transaction_items (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    sku            TEXT NOT NULL,
    quantity       INT           NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,  -- price snapshot at scan time
    PRIMARY KEY (transaction_id, sku)
);

-- The transactional outbox. The scan statement appends one row here in the
-- same atomic statement that adds the unit to a basket; OutboxRelay publishes
-- the rows, in seq order, to Kafka for analytics-service.
CREATE TABLE scan_events (
    seq        BIGSERIAL   PRIMARY KEY,
    sku        TEXT        NOT NULL,
    scanned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Single row: the current reset epoch, and how far into the outbox the relay
-- has published (and had acknowledged) under it.
CREATE TABLE relay_state (
    id            INT    PRIMARY KEY CHECK (id = 1),
    epoch         BIGINT NOT NULL,
    published_seq BIGINT NOT NULL
);

INSERT INTO relay_state (id, epoch, published_seq) VALUES (1, 0, 0);
