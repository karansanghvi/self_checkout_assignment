package com.school.selfcheckout.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The SQL half of a reset, kept in its own bean so {@code @Transactional}
 * actually applies -- calling a transactional method from a sibling method on
 * the same bean bypasses the proxy and silently runs without a transaction.
 *
 * Mirrors scripts/reset.sql. TRUNCATE plus generate_series keeps a full reset
 * sub-second regardless of how much data the previous run produced.
 */
@Repository
public class ResetRepository {

    private final JdbcTemplate jdbc;

    public ResetRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void reseed(int catalogSize, int stockPerItem) {
        jdbc.execute("""
                TRUNCATE transaction_items,
                         transactions,
                         low_stock_alerts,
                         popular_item_entries,
                         popular_item_windows,
                         stock_shortfalls
                  RESTART IDENTITY CASCADE
                """);

        // Drop anything outside the requested catalog size, so shrinking the
        // catalog between runs works as well as growing it.
        jdbc.update(
                "DELETE FROM inventory WHERE sku NOT IN "
                        + "(SELECT 'SKU-' || LPAD(i::text, 6, '0') FROM generate_series(1, ?) AS i)",
                catalogSize);
        jdbc.update(
                "DELETE FROM items WHERE sku NOT IN "
                        + "(SELECT 'SKU-' || LPAD(i::text, 6, '0') FROM generate_series(1, ?) AS i)",
                catalogSize);

        jdbc.update("""
                INSERT INTO items (sku, name, price)
                SELECT 'SKU-' || LPAD(i::text, 6, '0'),
                       'Item ' || i,
                       ROUND((0.5 + (i % 47) * 0.35)::numeric, 2)
                  FROM generate_series(1, ?) AS i
                ON CONFLICT (sku) DO NOTHING
                """, catalogSize);

        jdbc.update("""
                INSERT INTO inventory (sku, stock, initial_stock)
                SELECT sku, ?, ?
                  FROM items
                ON CONFLICT (sku) DO UPDATE
                   SET stock         = EXCLUDED.stock,
                       initial_stock = EXCLUDED.initial_stock,
                       updated_at    = now()
                """, stockPerItem, stockPerItem);
    }
}
