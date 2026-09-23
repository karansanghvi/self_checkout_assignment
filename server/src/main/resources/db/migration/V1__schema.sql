-- Self-checkout schema.
--
-- Design note: the catalog (items) and the stock counter (inventory) are
-- deliberately separate tables. Inventory rows are the hot, contended rows --
-- keeping them narrow means less WAL per update, and it means catalog reads
-- never touch a row that a completing transaction holds a lock on.

CREATE TABLE items (
    sku   TEXT PRIMARY KEY,
    name  TEXT          NOT NULL,
    price NUMERIC(10,2) NOT NULL
);

CREATE TABLE inventory (
    sku           TEXT PRIMARY KEY REFERENCES items(sku),
    stock         INT NOT NULL CHECK (stock >= 0),  -- DB-level backstop against overselling
    initial_stock INT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id            UUID PRIMARY KEY,
    station_id    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('OPEN', 'COMPLETED', 'CANCELLED')),
    item_count    INT           NOT NULL DEFAULT 0,
    running_total NUMERIC(12,2) NOT NULL DEFAULT 0,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

-- Aggregated per SKU rather than one row per scanned unit: this is what makes
-- receipt lines a plain SELECT, and keeps the scan path a single upsert.
CREATE TABLE transaction_items (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    sku            TEXT NOT NULL REFERENCES items(sku),
    quantity       INT           NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,  -- price snapshot at scan time
    PRIMARY KEY (transaction_id, sku)
);

-- One row per threshold crossing, so triggered_at is a real timestamp rather
-- than "whenever someone happened to query".
CREATE TABLE low_stock_alerts (
    id            BIGSERIAL PRIMARY KEY,
    sku           TEXT NOT NULL REFERENCES items(sku),
    current_stock INT  NOT NULL,
    threshold     INT  NOT NULL,
    triggered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_low_stock_alerts_sku_time ON low_stock_alerts (sku, triggered_at DESC);

-- Persisted top-N snapshot, one per hop of the window.
CREATE TABLE popular_item_windows (
    id             BIGSERIAL PRIMARY KEY,
    window_start   BIGINT NOT NULL,
    window_end     BIGINT NOT NULL,
    window_size    INT    NOT NULL,
    slide_interval INT    NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE popular_item_entries (
    window_id  BIGINT NOT NULL REFERENCES popular_item_windows(id) ON DELETE CASCADE,
    rank       INT    NOT NULL,
    sku        TEXT   NOT NULL REFERENCES items(sku),
    scan_count INT    NOT NULL,
    PRIMARY KEY (window_id, rank)
);

-- Records units that could not be fulfilled because stock had already hit zero.
-- The OpenAPI contract gives /complete no insufficient-stock error path, so we
-- must still return 200 -- but we refuse to lose the information. This table is
-- what lets us prove the honest invariant:
--     initial_stock - final_stock == completed_line_items - shortfall
CREATE TABLE stock_shortfalls (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL,
    sku            TEXT NOT NULL REFERENCES items(sku),
    requested      INT  NOT NULL,
    fulfilled      INT  NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
