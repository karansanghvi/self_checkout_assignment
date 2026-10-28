-- Single row: the reset epoch catalog_db is in, and the stock level that
-- reset asked for. catalog-service republishes its snapshot at startup, and
-- inventory-service reseeds from the snapshot, so the stock level travels
-- with it.
CREATE TABLE catalog_state (
    id             INT    PRIMARY KEY CHECK (id = 1),
    epoch          BIGINT NOT NULL,
    stock_per_item INT    NOT NULL
);

INSERT INTO catalog_state (id, epoch, stock_per_item) VALUES (1, 0, 10000);
