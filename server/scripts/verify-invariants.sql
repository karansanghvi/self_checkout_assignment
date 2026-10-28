-- Correctness check for grading, independent of any performance number.
--
-- The data lives in four databases, and Postgres cannot join across
-- databases. So this script \connects to each in turn, carries scalars
-- between them in psql variables (\gset), and copies checkout's per-SKU
-- sales into a temp table in inventory_db for the per-SKU cross-checks.
--
-- Since week 6, inventory learns about a sale only from the
-- TransactionCompleted event on Kafka. Sections 3 and 5 are therefore the
-- end-to-end check of event delivery: every sale checkout recorded must have
-- been applied by inventory exactly once.
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

\set ON_ERROR_STOP 1
\pset footer off

-- --- checkout_db ----------------------------------------------------------
\connect checkout_db
SELECT count(*) FILTER (WHERE status = 'COMPLETED')  AS completed,
       count(*) FILTER (WHERE status = 'OPEN')       AS open
  FROM transactions
\gset ck_
SELECT COALESCE(SUM(ti.quantity), 0) AS units_sold
  FROM transaction_items ti
  JOIN transactions t ON t.id = ti.transaction_id
 WHERE t.status = 'COMPLETED'
\gset ck_
SELECT (SELECT COALESCE(SUM(quantity), 0) FROM transaction_items)                     AS units_scanned,
       (SELECT count(*) FROM outbox)                                                   AS outbox_rows,
       (SELECT count(*) FROM outbox WHERE topic = 'scan-events')                       AS scan_events,
       (SELECT count(*) FROM outbox WHERE topic = 'transaction-completed')             AS completion_events,
       (SELECT COALESCE(max(seq), 0) FROM outbox)                                      AS outbox_max_seq,
       (SELECT published_seq FROM relay_state)                                         AS published_seq,
       (SELECT epoch FROM relay_state)                                                 AS epoch
\gset ck_

-- --- inventory_db ---------------------------------------------------------
\connect inventory_db

\echo '== 1. Stock must never be negative ================================'
SELECT count(*) AS negative_stock_rows FROM inventory WHERE stock < 0;

\echo ''
\echo '== 2. inventory_db is self-consistent (should return zero rows) ==='
\echo '   initial_stock - stock == SUM(fulfilled) over every stock commit'
SELECT inv.sku, inv.initial_stock, inv.stock,
       inv.initial_stock - inv.stock    AS decremented,
       COALESCE(c.fulfilled, 0)         AS committed_fulfilled
  FROM inventory inv
  LEFT JOIN (SELECT sku, SUM(fulfilled)::bigint AS fulfilled
               FROM stock_commit_lines GROUP BY sku) c ON c.sku = inv.sku
 WHERE inv.initial_stock - inv.stock <> COALESCE(c.fulfilled, 0)
 ORDER BY inv.sku;

-- Checkout's units sold per SKU, copied across the database boundary.
CREATE TEMP TABLE checkout_sold (sku TEXT PRIMARY KEY, units BIGINT NOT NULL);
\copy checkout_sold FROM PROGRAM 'psql -X -q -d checkout_db -c "COPY (SELECT ti.sku, SUM(ti.quantity) FROM transaction_items ti JOIN transactions t ON t.id = ti.transaction_id WHERE t.status = ''COMPLETED'' GROUP BY ti.sku) TO STDOUT"'

\echo ''
\echo '== 3. Events were applied exactly once (should return zero rows) =='
\echo '   per SKU: units inventory applied from TransactionCompleted == units checkout sold'
SELECT COALESCE(c.sku, s.sku)     AS sku,
       COALESCE(c.requested, 0)   AS inventory_applied,
       COALESCE(s.units, 0)       AS checkout_sold
  FROM (SELECT sku, SUM(requested)::bigint AS requested
          FROM stock_commit_lines GROUP BY sku) c
  FULL JOIN checkout_sold s ON s.sku = c.sku
 WHERE COALESCE(c.requested, 0) <> COALESCE(s.units, 0)
 ORDER BY 1;

\echo ''
\echo '== 4. Per-SKU accounting, README form (should return zero rows) ==='
\echo '   initial_stock - final_stock == completed_line_items - shortfall'
SELECT inv.sku,
       inv.initial_stock,
       inv.stock                              AS final_stock,
       inv.initial_stock - inv.stock          AS actually_decremented,
       COALESCE(s.units, 0)                   AS completed_line_items,
       COALESCE(sh.units_short, 0)            AS unfulfilled
  FROM inventory inv
  LEFT JOIN checkout_sold s ON s.sku = inv.sku
  LEFT JOIN (SELECT sku, SUM(requested - fulfilled)::bigint AS units_short
               FROM stock_shortfalls GROUP BY sku) sh ON sh.sku = inv.sku
 WHERE inv.initial_stock - inv.stock
       <> COALESCE(s.units, 0) - COALESCE(sh.units_short, 0)
 ORDER BY inv.sku;

\echo ''
\echo '== 5. Completion events: checkout outbox -> Kafka -> inventory ===='
SELECT :ck_completed                                        AS completed_transactions,
       :ck_completion_events                                AS completion_events_in_outbox,
       (SELECT count(*) FROM stock_commits)                 AS stock_commits_applied,
       (SELECT count(*) FROM stock_commits) = :ck_completed AS one_commit_per_sale,
       :ck_open                                             AS abandoned_open_transactions,
       :ck_units_sold                                       AS total_units_sold,
       (SELECT COALESCE(SUM(initial_stock - stock), 0) FROM inventory)        AS total_units_decremented,
       (SELECT COALESCE(SUM(requested - fulfilled), 0) FROM stock_shortfalls) AS total_units_unfulfilled,
       (SELECT count(*) FROM inventory WHERE stock = 0)     AS skus_exhausted,
       (SELECT epoch FROM inventory_state) = :ck_epoch      AS same_epoch_as_checkout;

\echo ''
\echo '== 6. Hottest SKUs by units sold =================================='
SELECT s.sku, s.units AS units_sold, inv.stock AS remaining_stock
  FROM checkout_sold s
  JOIN inventory inv ON inv.sku = s.sku
 ORDER BY s.units DESC
 LIMIT 10;

-- --- analytics_db ---------------------------------------------------------
\connect analytics_db

\echo ''
\echo '== 7. Scan events: checkout outbox -> Kafka -> analytics =========='
-- Every accepted scan is logged exactly once, in the same statement that put
-- it in a basket, so the scan events must match the basket lines unit for
-- unit. The relay must have published the whole outbox, and analytics
-- persists one window per slide_interval scans: floor(scans / slide_interval).
SELECT :ck_units_scanned                         AS units_in_baskets,
       :ck_scan_events                           AS scan_events,
       :ck_units_scanned = :ck_scan_events       AS events_match_baskets,
       :ck_outbox_max_seq - :ck_outbox_rows      AS seq_gaps,
       :ck_published_seq = :ck_outbox_max_seq    AS relay_published_all,
       (SELECT count(*) FROM popular_item_windows)            AS windows_persisted,
       :ck_scan_events / COALESCE((SELECT max(slide_interval) FROM popular_item_windows), 500)
                                                 AS windows_expected,
       (SELECT COALESCE(max(window_end), 0) FROM popular_item_windows) AS last_window_end,
       (SELECT epoch FROM analytics_state) = :ck_epoch AS same_epoch_as_checkout;
