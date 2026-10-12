-- Week 4: the tables that let separately deployed services coordinate through
-- the shared database instead of calling each other.

-- The scan feed from checkout-service to analytics-service.
--
-- Checkout appends one row per scan, inside the same statement that adds the
-- unit to the basket, so a scan is logged exactly when it is accepted.
-- Analytics polls it in seq order and feeds its popularity pipeline.
--
-- No foreign key to items: checkout has already validated the SKU against the
-- catalog, and an FK check on the hottest insert in the system buys nothing.
CREATE TABLE scan_events (
    seq        BIGSERIAL   PRIMARY KEY,
    sku        TEXT        NOT NULL,
    scanned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reset generation counter, single row.
--
-- admin-service bumps it in the same transaction that wipes and reseeds. Any
-- service holding in-memory state derived from the database (the catalog
-- cache, the popularity window) watches it and rebuilds when it changes.
-- That replaces the in-process "reset the pipeline, then reload the cache"
-- calls the monolith made from ResetService.
CREATE TABLE system_epoch (
    id       INT         PRIMARY KEY CHECK (id = 1),
    epoch    BIGINT      NOT NULL,
    reset_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO system_epoch (id, epoch) VALUES (1, 1);

-- One persisted window per hop. analytics-service rebuilds its window on
-- startup by replaying scan_events from the start of the epoch, which
-- re-derives windows it may already have written; this makes that replay
-- idempotent instead of duplicating them.
--
-- A database that saw several runs without a reset in between can already
-- hold repeated window_end values; keep the newest of each so the index builds.
DELETE FROM popular_item_windows older
 USING popular_item_windows newer
 WHERE older.window_end = newer.window_end
   AND older.id < newer.id;

CREATE UNIQUE INDEX uq_popular_item_windows_end ON popular_item_windows (window_end);
