-- analytics_db: owned by analytics-service, and by nobody else.

-- Persisted top-N snapshot, one per hop of the window. window_end is unique
-- within a run: replaying the topic after a restart re-derives windows that
-- may already be here, and this makes that replay idempotent.
CREATE TABLE popular_item_windows (
    id             BIGSERIAL PRIMARY KEY,
    window_start   BIGINT NOT NULL,
    window_end     BIGINT NOT NULL UNIQUE,
    window_size    INT    NOT NULL,
    slide_interval INT    NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- No foreign key to items: they live in catalog_db.
CREATE TABLE popular_item_entries (
    window_id  BIGINT NOT NULL REFERENCES popular_item_windows(id) ON DELETE CASCADE,
    rank       INT    NOT NULL,
    sku        TEXT   NOT NULL,
    scan_count INT    NOT NULL,
    PRIMARY KEY (window_id, rank)
);

-- Single row: the current run's epoch, and the scan-events topic offset at
-- which that run's events begin. Replaces week 4's shared system_epoch table.
CREATE TABLE analytics_state (
    id           INT    PRIMARY KEY CHECK (id = 1),
    epoch        BIGINT NOT NULL,
    start_offset BIGINT NOT NULL,
    reset_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO analytics_state (id, epoch, start_offset) VALUES (1, 0, 0);
