-- catalog_db: owned by catalog-service, and by nobody else.
--
-- Other services that need item names or prices replicate this table over
-- GET /items (see CatalogReplica) instead of reading it.

CREATE TABLE items (
    sku   TEXT PRIMARY KEY,
    name  TEXT          NOT NULL,
    price NUMERIC(10,2) NOT NULL
);
