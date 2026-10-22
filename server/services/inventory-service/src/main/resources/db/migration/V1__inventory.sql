-- inventory_db: owned by inventory-service, and by nobody else.
--
-- Stock rows are the hot, contended rows of the whole system -- every station
-- completing a basket fights over them -- so they stay narrow. The item name
-- is a copy taken from catalog-service at reset, so that low-stock alerts can
-- name a SKU without a call to another service.

CREATE TABLE inventory (
    sku           TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    stock         INT  NOT NULL CHECK (stock >= 0),  -- DB-level backstop against overselling
    initial_stock INT  NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per completed basket: the idempotency key of the completion saga.
-- checkout-service may send the same commit more than once (a retry after a
-- timeout, or its recovery sweeper resuming a saga); the primary key is what
-- makes the second attempt a no-op.
CREATE TABLE stock_commits (
    transaction_id UUID PRIMARY KEY,
    committed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- What each commit asked for and what it got. Answers a replayed commit, and
-- lets inventory_db prove on its own that
--     initial_stock - stock == SUM(fulfilled)
-- for every SKU, without looking at checkout's database.
CREATE TABLE stock_commit_lines (
    transaction_id UUID NOT NULL REFERENCES stock_commits(transaction_id) ON DELETE CASCADE,
    sku            TEXT NOT NULL REFERENCES inventory(sku),
    requested      INT  NOT NULL CHECK (requested > 0),
    fulfilled      INT  NOT NULL CHECK (fulfilled >= 0 AND fulfilled <= requested),
    PRIMARY KEY (transaction_id, sku)
);

-- One row per threshold crossing, so triggered_at is a real timestamp rather
-- than "whenever someone happened to query".
CREATE TABLE low_stock_alerts (
    id            BIGSERIAL PRIMARY KEY,
    sku           TEXT NOT NULL REFERENCES inventory(sku),
    current_stock INT  NOT NULL,
    threshold     INT  NOT NULL,
    triggered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_low_stock_alerts_sku_time ON low_stock_alerts (sku, triggered_at DESC);

-- Units that could not be fulfilled because stock had already hit zero. The
-- contract gives /complete no insufficient-stock error path, so the basket
-- still completes -- but the shortfall is recorded, not lost.
CREATE TABLE stock_shortfalls (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL,
    sku            TEXT NOT NULL REFERENCES inventory(sku),
    requested      INT  NOT NULL,
    fulfilled      INT  NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
