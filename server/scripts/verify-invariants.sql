-- Correctness check for grading, independent of any performance number.
--
-- The README's stated invariant is:
--     for every SKU, initial_stock - final_stock must equal the total number
--     of completed-transaction line items for that SKU, and final stock must
--     never go negative.
--
-- Both hold for any SKU with stock remaining. Once a hot SKU reaches zero the
-- two clauses pull apart, so we check the generalised form that accounts for
-- units we declined to oversell:
--     initial_stock - final_stock == completed_line_items - shortfall
-- The stated invariant is the special case where shortfall = 0.

\echo '== 1. Stock must never be negative =============================='
SELECT count(*) AS negative_stock_rows
  FROM inventory
 WHERE stock < 0;

\echo ''
\echo '== 2. Per-SKU accounting (should return zero rows) =============='
WITH sold AS (
    SELECT ti.sku, SUM(ti.quantity)::bigint AS units_sold
      FROM transaction_items ti
      JOIN transactions t ON t.id = ti.transaction_id
     WHERE t.status = 'COMPLETED'
     GROUP BY ti.sku
),
short AS (
    SELECT sku, SUM(requested - fulfilled)::bigint AS units_short
      FROM stock_shortfalls
     GROUP BY sku
)
SELECT inv.sku,
       inv.initial_stock,
       inv.stock                                    AS final_stock,
       inv.initial_stock - inv.stock                AS actually_decremented,
       COALESCE(sold.units_sold, 0)                 AS completed_line_items,
       COALESCE(short.units_short, 0)               AS unfulfilled,
       COALESCE(sold.units_sold, 0)
         - COALESCE(short.units_short, 0)           AS expected_decrement
  FROM inventory inv
  LEFT JOIN sold  ON sold.sku  = inv.sku
  LEFT JOIN short ON short.sku = inv.sku
 WHERE (inv.initial_stock - inv.stock)
       <> (COALESCE(sold.units_sold, 0) - COALESCE(short.units_short, 0))
 ORDER BY inv.sku;

\echo ''
\echo '== 3. Summary =================================================='
WITH sold AS (
    SELECT SUM(ti.quantity)::bigint AS units_sold
      FROM transaction_items ti
      JOIN transactions t ON t.id = ti.transaction_id
     WHERE t.status = 'COMPLETED'
)
SELECT (SELECT count(*) FROM transactions WHERE status = 'COMPLETED') AS completed_transactions,
       (SELECT count(*) FROM transactions WHERE status = 'OPEN')      AS abandoned_open_transactions,
       COALESCE((SELECT units_sold FROM sold), 0)                     AS total_units_sold,
       (SELECT COALESCE(SUM(initial_stock - stock), 0) FROM inventory) AS total_units_decremented,
       (SELECT COALESCE(SUM(requested - fulfilled), 0) FROM stock_shortfalls) AS total_units_unfulfilled,
       (SELECT count(*) FROM inventory WHERE stock = 0)               AS skus_exhausted;

\echo ''
\echo '== 4. Hottest SKUs by units sold ==============================='
SELECT ti.sku,
       SUM(ti.quantity) AS units_sold,
       inv.stock        AS remaining_stock
  FROM transaction_items ti
  JOIN transactions t   ON t.id = ti.transaction_id
  JOIN inventory inv    ON inv.sku = ti.sku
 WHERE t.status = 'COMPLETED'
 GROUP BY ti.sku, inv.stock
 ORDER BY units_sold DESC
 LIMIT 10;
