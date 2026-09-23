-- Wipe all run state and reseed the catalog + inventory.
--
-- Parameterised via psql variables, with defaults matching the assignment:
--   psql -v catalog_size=2000 -v stock_per_item=10000 -f reset.sql
--
-- TRUNCATE (not DELETE) so this stays sub-second no matter how many
-- transactions the previous load run produced.

\if :{?catalog_size}
\else
  \set catalog_size 2000
\endif

\if :{?stock_per_item}
\else
  \set stock_per_item 10000
\endif

BEGIN;

TRUNCATE transaction_items,
         transactions,
         low_stock_alerts,
         popular_item_entries,
         popular_item_windows,
         stock_shortfalls
  RESTART IDENTITY CASCADE;

-- Rebuild the catalog only if the requested size differs from what is there,
-- so the common case (same size every run) skips the churn entirely.
DELETE FROM inventory
 WHERE sku NOT IN (SELECT 'SKU-' || LPAD(i::text, 6, '0')
                     FROM generate_series(1, :catalog_size) AS i);
DELETE FROM items
 WHERE sku NOT IN (SELECT 'SKU-' || LPAD(i::text, 6, '0')
                     FROM generate_series(1, :catalog_size) AS i);

INSERT INTO items (sku, name, price)
SELECT 'SKU-' || LPAD(i::text, 6, '0'),
       'Item ' || i,
       ROUND((0.5 + (i % 47) * 0.35)::numeric, 2)
  FROM generate_series(1, :catalog_size) AS i
ON CONFLICT (sku) DO NOTHING;

INSERT INTO inventory (sku, stock, initial_stock)
SELECT sku, :stock_per_item, :stock_per_item
  FROM items
ON CONFLICT (sku) DO UPDATE
   SET stock         = EXCLUDED.stock,
       initial_stock = EXCLUDED.initial_stock,
       updated_at    = now();

COMMIT;

SELECT (SELECT count(*) FROM items)                     AS items,
       (SELECT count(*) FROM inventory)                 AS inventory_rows,
       (SELECT min(stock) FROM inventory)               AS min_stock,
       (SELECT max(stock) FROM inventory)               AS max_stock,
       (SELECT count(*) FROM transactions)              AS transactions;
