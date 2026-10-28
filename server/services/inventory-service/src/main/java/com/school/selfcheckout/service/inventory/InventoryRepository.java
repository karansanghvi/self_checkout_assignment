package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
     * Takes a whole batch's demand out of stock in one statement, never below
     * zero, and reports each SKU's stock before and after.
     *
     * This is what replaced week 5's per-basket {@code UPDATE ... WHERE stock
     * >= ?}. Completions now arrive as a stream that one consumer applies in
     * batches, so the demand of hundreds of baskets is summed per SKU first,
     * and each SKU's row is updated once:
     *
     *     stock = stock - LEAST(stock, demand)
     *
     * The clamp is the no-overselling rule: the subtraction can never exceed
     * what is there. It stays safe under concurrency, should there ever be
     * more than one writer: {@code locked} takes the row locks with
     * {@code FOR UPDATE}, which under READ COMMITTED waits for any concurrent
     * writer and then returns the row's newest committed version, so the
     * value the new stock is computed from is current, not a stale snapshot.
     * {@code CHECK (stock >= 0)} remains the backstop.
     *
     * Locks are taken in descending SKU order, the same global order as in
     * week 5, so two writers could never deadlock.
     *
     * @return one entry per SKU that has an inventory row; a SKU without one
     *         is simply absent
     */
    public List<StockChange> takeStock(Map<String, Integer> demand) {
        String[] skus = demand.keySet().toArray(String[]::new);
        Integer[] quantities = demand.values().toArray(Integer[]::new);
        return jdbc.query(
                con -> {
                    PreparedStatement ps = con.prepareStatement("""
                            WITH demand AS (
                                SELECT * FROM unnest(CAST(? AS text[]), CAST(? AS int[])) AS d(sku, qty)
                            ),
                            locked AS (
                                SELECT i.sku, i.stock
                                  FROM inventory i JOIN demand d ON d.sku = i.sku
                                 ORDER BY i.sku DESC
                                   FOR UPDATE OF i
                            )
                            UPDATE inventory i
                               SET stock = locked.stock - LEAST(locked.stock, d.qty), updated_at = now()
                              FROM locked JOIN demand d ON d.sku = locked.sku
                             WHERE i.sku = locked.sku
                            RETURNING i.sku, locked.stock AS before, i.stock AS after
                            """);
                    ps.setArray(1, con.createArrayOf("text", skus));
                    ps.setArray(2, con.createArrayOf("integer", quantities));
                    return ps;
                },
                (rs, rowNum) -> new StockChange(rs.getString("sku"), rs.getInt("before"), rs.getInt("after")));
    }

    /**
     * Claims each transaction id as the idempotency key of its stock commit.
     *
     * The relay delivers at least once, so the same TransactionCompleted can
     * arrive twice. The offset saved with each batch already stops a replay
     * after a crash; this primary key is the second line of defence, against
     * a duplicate <i>record</i> -- the relay republishing a batch Kafka had
     * in fact stored.
     *
     * @return the ids not seen before: the transactions this batch must apply
     */
    public Set<UUID> claimCommits(Collection<UUID> transactionIds) {
        UUID[] ids = transactionIds.toArray(UUID[]::new);
        return new HashSet<>(jdbc.query(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO stock_commits (transaction_id) SELECT unnest(CAST(? AS uuid[])) "
                                    + "ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id");
                    ps.setArray(1, con.createArrayOf("uuid", ids));
                    return ps;
                },
                (rs, rowNum) -> rs.getObject("transaction_id", UUID.class)));
    }

    public void insertCommitLines(List<CommitLine> lines) {
        jdbc.batchUpdate(
                "INSERT INTO stock_commit_lines (transaction_id, sku, requested, fulfilled) VALUES (?, ?, ?, ?)",
                lines, 1_000,
                (ps, line) -> {
                    ps.setObject(1, line.transactionId());
                    ps.setString(2, line.sku());
                    ps.setInt(3, line.requested());
                    ps.setInt(4, line.fulfilled());
                });
    }

    public void insertShortfalls(List<CommitLine> lines) {
        jdbc.batchUpdate(
                "INSERT INTO stock_shortfalls (transaction_id, sku, requested, fulfilled) VALUES (?, ?, ?, ?)",
                lines, 1_000,
                (ps, line) -> {
                    ps.setObject(1, line.transactionId());
                    ps.setString(2, line.sku());
                    ps.setInt(3, line.requested());
                    ps.setInt(4, line.fulfilled());
                });
    }

    public void insertLowStockAlerts(List<StockChange> crossings, int threshold) {
        jdbc.batchUpdate(
                "INSERT INTO low_stock_alerts (sku, current_stock, threshold) VALUES (?, ?, ?)",
                crossings, 1_000,
                (ps, change) -> {
                    ps.setString(1, change.sku());
                    ps.setInt(2, change.after());
                    ps.setInt(3, threshold);
                });
    }

    public InventoryState loadState() {
        return jdbc.queryForObject(
                "SELECT epoch, next_offset FROM inventory_state WHERE id = 1",
                (rs, rowNum) -> new InventoryState(rs.getLong("epoch"), rs.getLong("next_offset")));
    }

    public void saveNextOffset(long nextOffset) {
        jdbc.update("UPDATE inventory_state SET next_offset = ? WHERE id = 1", nextOffset);
    }

    public Set<String> findAllSkus() {
        return new HashSet<>(jdbc.queryForList("SELECT sku FROM inventory", String.class));
    }

    /**
     * Wipes run state, reseeds one stock row per catalog item, and moves to
     * {@code epoch}, in one transaction.
     *
     * The item name is copied in alongside the stock: low-stock alerts must
     * name the SKU, and the name lives in another service's database.
     * Copying it from the catalog snapshot that triggered this reset keeps
     * {@code GET /inventory/low-stock} a local query.
     *
     * The topic offset is left alone. The consumer carries on from where it
     * is, dropping any completion from the old epoch still ahead of it.
     */
    @Transactional
    public void reseed(long epoch, List<CatalogItem> items, int stockPerItem) {
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
        jdbc.update("UPDATE inventory_state SET epoch = ? WHERE id = 1", epoch);
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
