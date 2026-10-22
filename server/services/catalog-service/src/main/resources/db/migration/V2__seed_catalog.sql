-- Seed 2,000 items.
--
-- The generator mirrors the reference MockServer exactly (SKU-%06d, "Item N",
-- price = 0.5 + (i % 47) * 0.35) so that our load-test reports stay directly
-- comparable with the reference run in reference-repo/load-client/reports/.
--
-- Pure SQL via generate_series: seeding is sub-second, with no 2,000-round-trip
-- client loop.

INSERT INTO items (sku, name, price)
SELECT 'SKU-' || LPAD(i::text, 6, '0'),
       'Item ' || i,
       ROUND((0.5 + (i % 47) * 0.35)::numeric, 2)
  FROM generate_series(1, 2000) AS i
ON CONFLICT (sku) DO NOTHING;

-- Stock is not seeded here: it lives in inventory_db, which inventory-service
-- fills from GET /items when admin-service resets the system.
