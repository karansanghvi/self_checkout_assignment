package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.InternalDtos.StockCommitOutcome;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

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
     *
     * In week 4 this ran inside checkout-service through a shared library.
     * Now only inventory-service can reach this table, so the statement that
     * prevents overselling is executed by the service that owns the row --
     * checkout asks for the decrement over HTTP and never sees the stock.
     *
     * @return the outcome, or {@code null} if the SKU has no inventory row
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

        if (slow.isEmpty()) {
            return null;
        }
        return new DecrementResult(slow.get(0), 0);
    }

    /**
     * Claims {@code transactionId} as the idempotency key of a stock commit.
     *
     * Two concurrent commits for the same transaction both try this INSERT;
     * the second blocks on the primary key until the first finishes, then
     * either conflicts (the first committed: nothing to do) or inserts (the
     * first rolled back: this one does the work). Either way the lines are
     * applied exactly once.
     *
     * @return true if this call owns the commit, false if it was already applied
     */
    public boolean claimCommit(UUID transactionId) {
        return !jdbc.queryForList(
                "INSERT INTO stock_commits (transaction_id) VALUES (?) "
                        + "ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id",
                UUID.class, transactionId).isEmpty();
    }

    public void insertCommitLines(UUID transactionId, List<StockCommitOutcome> lines) {
        jdbc.batchUpdate(
                "INSERT INTO stock_commit_lines (transaction_id, sku, requested, fulfilled) VALUES (?, ?, ?, ?)",
                lines, lines.size(),
                (ps, line) -> {
                    ps.setObject(1, transactionId);
                    ps.setString(2, line.sku());
                    ps.setInt(3, line.requested());
                    ps.setInt(4, line.fulfilled());
                });
    }

    /** What an earlier commit of this transaction did, for answering a retry. */
    public List<StockCommitOutcome> findCommitLines(UUID transactionId) {
        return jdbc.query(
                "SELECT sku, requested, fulfilled FROM stock_commit_lines "
                        + "WHERE transaction_id = ? ORDER BY sku DESC",
                (rs, rowNum) -> new StockCommitOutcome(
                        rs.getString("sku"), rs.getInt("requested"), rs.getInt("fulfilled")),
                transactionId);
    }

    /**
     * Wipes run state and reseeds one stock row per catalog item.
     *
     * The item name is copied in alongside the stock: low-stock alerts must
     * name the SKU, and the name now lives in another service's database.
     * Copying it at reset, from the catalog snapshot admin-service's reset
     * just produced, keeps {@code GET /inventory/low-stock} a local query.
     */
    @Transactional
    public void reseed(List<CatalogItem> items, int stockPerItem) {
        jdbc.execute("""
                TRUNCATE stock_commit_lines,
                         stock_commits,
                         low_stock_alerts,
                         stock_shortfalls,
                         inventory
                  RESTART IDENTITY CASCADE
                """);
        jdbc.batchUpdate(
                "INSERT INTO inventory (sku, name, stock, initial_stock) VALUES (?, ?, ?, ?)",
                items, 500,
                (ps, item) -> {
                    ps.setString(1, item.sku());
                    ps.setString(2, item.name());
                    ps.setInt(3, stockPerItem);
                    ps.setInt(4, stockPerItem);
                });
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
                SELECT inv.sku, inv.name, inv.stock, a.triggered_at
                  FROM inventory inv
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
