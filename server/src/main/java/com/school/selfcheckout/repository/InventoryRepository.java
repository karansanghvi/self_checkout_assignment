package com.school.selfcheckout.repository;

import com.school.selfcheckout.domain.DecrementResult;
import com.school.selfcheckout.domain.LowStockRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

@Repository
public class InventoryRepository {

    private static final RowMapper<LowStockRow> LOW_STOCK_MAPPER = (rs, rowNum) -> {
        Timestamp triggeredAt = rs.getTimestamp("triggered_at");
        return new LowStockRow(
                rs.getString("sku"),
                rs.getString("name"),
                rs.getInt("stock"),
                triggeredAt == null ? null : triggeredAt.toInstant());
    };

    private final JdbcTemplate jdbc;

    public InventoryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Decrements one SKU by {@code quantity}, never below zero.
     *
     * FAST PATH -- a single conditional UPDATE, which is the whole answer to
     * the concurrency problem this assignment is built around:
     *
     *     UPDATE inventory SET stock = stock - q WHERE sku = ? AND stock >= q
     *
     * There is no read-then-write window here. Under READ COMMITTED, when a
     * concurrent transaction has already modified this row, Postgres blocks on
     * the row lock and then re-evaluates the WHERE clause against the newly
     * committed version before deciding whether to write. The check and the
     * write are the same operation, so two stations racing for the last unit
     * cannot both win: one sees 1 row affected, the other sees 0.
     *
     * SLOW PATH -- reached only when stock is about to run out, which under the
     * load client's Zipf sampling happens for a handful of very hot SKUs. Here
     * we take what remains and report the shortfall, since the contract gives
     * /complete no insufficient-stock error path. SELECT ... FOR UPDATE and the
     * UPDATE are fused into one statement so the rare path still costs one
     * round trip; the FOR UPDATE forces a re-read of the latest committed row
     * under lock, so the absolute value we write is computed from current data.
     */
    public DecrementResult decrement(String sku, int quantity) {
        List<Integer> fast = jdbc.query(
                "UPDATE inventory SET stock = stock - ?, updated_at = now() "
                        + "WHERE sku = ? AND stock >= ? "
                        + "RETURNING stock",
                (rs, rowNum) -> rs.getInt("stock"),
                quantity, sku, quantity);

        if (!fast.isEmpty()) {
            return new DecrementResult(quantity, fast.get(0));
        }

        List<Integer> slow = jdbc.query(
                """
                WITH locked AS (
                    SELECT sku, stock FROM inventory WHERE sku = ? FOR UPDATE
                )
                UPDATE inventory i
                   SET stock = 0, updated_at = now()
                  FROM locked
                 WHERE i.sku = locked.sku
                RETURNING locked.stock AS taken
                """,
                (rs, rowNum) -> rs.getInt("taken"),
                sku);

        int taken = slow.isEmpty() ? 0 : slow.get(0);
        return new DecrementResult(taken, 0);
    }

    public void recordShortfall(UUID transactionId, String sku, int requested, int fulfilled) {
        jdbc.update(
                "INSERT INTO stock_shortfalls (transaction_id, sku, requested, fulfilled) VALUES (?, ?, ?, ?)",
                transactionId, sku, requested, fulfilled);
    }

    public void recordLowStockAlert(String sku, int currentStock, int threshold) {
        jdbc.update(
                "INSERT INTO low_stock_alerts (sku, current_stock, threshold) VALUES (?, ?, ?)",
                sku, currentStock, threshold);
    }

    /**
     * Current low-stock rows, with the real crossing time where we have one.
     *
     * The live {@code stock < threshold} scan is what makes the spec's optional
     * ?threshold= override meaningful -- a persisted alert row was triggered
     * against the configured threshold, so it cannot answer a question about a
     * different one. The LATERAL join supplies the genuine triggered_at when
     * the SKU did cross the configured threshold at some point.
     */
    public List<LowStockRow> findBelowThreshold(int threshold) {
        return jdbc.query(
                """
                SELECT inv.sku, it.name, inv.stock, a.triggered_at
                  FROM inventory inv
                  JOIN items it ON it.sku = inv.sku
                  LEFT JOIN LATERAL (
                      SELECT triggered_at
                        FROM low_stock_alerts la
                       WHERE la.sku = inv.sku
                       ORDER BY la.triggered_at DESC
                       LIMIT 1
                  ) a ON true
                 WHERE inv.stock < ?
                 ORDER BY inv.stock ASC, inv.sku ASC
                """,
                LOW_STOCK_MAPPER, threshold);
    }
}
