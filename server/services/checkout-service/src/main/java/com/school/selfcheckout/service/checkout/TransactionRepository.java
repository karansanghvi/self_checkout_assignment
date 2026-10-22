package com.school.selfcheckout.service.checkout;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class TransactionRepository {

    private static final RowMapper<TransactionRow> TX_MAPPER = (rs, rowNum) -> new TransactionRow(
            UUID.fromString(rs.getString("id")),
            rs.getString("station_id"),
            rs.getString("status"),
            rs.getInt("item_count"),
            rs.getBigDecimal("running_total"),
            rs.getTimestamp("started_at").toInstant(),
            rs.getTimestamp("completed_at") == null ? null : rs.getTimestamp("completed_at").toInstant());

    private static final RowMapper<TransactionLine> LINE_MAPPER = (rs, rowNum) -> new TransactionLine(
            rs.getString("sku"),
            rs.getInt("quantity"),
            rs.getBigDecimal("unit_price"));

    private final JdbcTemplate jdbc;

    public TransactionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Instant insertOpen(UUID id, String stationId) {
        Timestamp startedAt = jdbc.queryForObject(
                "INSERT INTO transactions (id, station_id, status) VALUES (?, ?, 'OPEN') "
                        + "RETURNING started_at",
                Timestamp.class, id, stationId);
        return startedAt == null ? Instant.now() : startedAt.toInstant();
    }

    public TransactionRow findById(UUID id) {
        try {
            return jdbc.queryForObject(
                    "SELECT id, station_id, status, item_count, running_total, started_at, completed_at "
                            + "FROM transactions WHERE id = ?",
                    TX_MAPPER, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    /** @return the transaction's status, or {@code null} if it does not exist. */
    public String findStatus(UUID id) {
        try {
            return jdbc.queryForObject("SELECT status FROM transactions WHERE id = ?", String.class, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    /**
     * Records one scanned unit and returns the updated basket totals.
     *
     * This is the hottest endpoint in the workload (~91% of all requests), so
     * it is deliberately a single round trip, doing three writes:
     *
     *   1. bump the denormalized counters on the transaction row,
     *   2. upsert the per-SKU basket line,
     *   3. append the scan to {@code scan_events}, this service's outbox,
     *      which {@link OutboxRelay} publishes to Kafka for analytics-service.
     *
     * The transaction UPDATE is the guard: both inserts select {@code FROM tx},
     * so if the transaction is missing or no longer OPEN, tx is empty and
     * nothing at all is written. A scan therefore reaches the analytics feed
     * exactly when it reaches a basket. Postgres runs every data-modifying
     * CTE exactly once and to completion, whether or not the outer query
     * reads its output.
     *
     * This is the transactional-outbox pattern: the event is written to
     * checkout's own database in the same atomic statement as the state
     * change it describes, and published to the broker afterwards. Publishing
     * to Kafka directly from here could not be atomic with the basket write --
     * a crash between the two would lose or invent a scan.
     *
     * No locking is needed. A transaction row is only ever touched by the one
     * station that owns it, so there is no contention to protect against --
     * unlike the inventory rows, which every station fights over.
     *
     * @return the new {@code (itemCount, runningTotal)}, or {@code null} if the
     *         transaction was missing or not OPEN.
     */
    public ScanTotals recordScan(UUID transactionId, String sku, BigDecimal unitPrice) {
        List<ScanTotals> result = jdbc.query(
                """
                WITH tx AS (
                    UPDATE transactions
                       SET item_count    = item_count + 1,
                           running_total = running_total + ?
                     WHERE id = ? AND status = 'OPEN'
                    RETURNING id, item_count, running_total
                ),
                line AS (
                    INSERT INTO transaction_items (transaction_id, sku, quantity, unit_price)
                    SELECT tx.id, CAST(? AS text), 1, CAST(? AS numeric) FROM tx
                    ON CONFLICT (transaction_id, sku)
                    DO UPDATE SET quantity = transaction_items.quantity + 1
                ),
                event AS (
                    INSERT INTO scan_events (sku)
                    SELECT CAST(? AS text) FROM tx
                )
                SELECT item_count, running_total FROM tx
                """,
                (rs, rowNum) -> new ScanTotals(rs.getInt("item_count"), rs.getBigDecimal("running_total")),
                unitPrice, transactionId, sku, unitPrice, sku);
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * Step 1 of the completion saga: atomically claims the transaction,
     * moving it OPEN -> COMPLETING.
     *
     * The {@code AND status = 'OPEN'} predicate is what makes concurrent
     * double-completion safe: two simultaneous /complete calls both try this
     * UPDATE, the second blocks on the row lock, then re-evaluates the
     * predicate against the committed 'COMPLETING' row and matches nothing.
     * {@code item_count > 0} refuses an empty basket in the same statement.
     *
     * COMPLETING is a durable marker that the saga is in flight. A scan cannot
     * land on it (recordScan requires OPEN), so the lines inventory is asked
     * to commit are final.
     *
     * @return the claimed transaction, or {@code null} if missing, not OPEN, or empty.
     */
    public TransactionRow claimForCompletion(UUID id) {
        List<TransactionRow> rows = jdbc.query(
                "UPDATE transactions SET status = 'COMPLETING', completing_since = now() "
                        + "WHERE id = ? AND status = 'OPEN' AND item_count > 0 "
                        + "RETURNING id, station_id, status, item_count, running_total, started_at, completed_at",
                TX_MAPPER, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Step 3 of the saga, after inventory has committed the stock:
     * COMPLETING -> COMPLETED.
     *
     * @return the completed transaction, or {@code null} if it was not
     *         COMPLETING -- in practice, another attempt finished it first.
     */
    public TransactionRow markCompleted(UUID id) {
        List<TransactionRow> rows = jdbc.query(
                "UPDATE transactions SET status = 'COMPLETED', completed_at = now() "
                        + "WHERE id = ? AND status = 'COMPLETING' "
                        + "RETURNING id, station_id, status, item_count, running_total, started_at, completed_at",
                TX_MAPPER, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * The saga's compensating step: COMPLETING -> OPEN, for when inventory
     * definitively refused the commit, so the station can fix the basket.
     */
    public boolean revertToOpen(UUID id) {
        return jdbc.update(
                "UPDATE transactions SET status = 'OPEN', completing_since = NULL "
                        + "WHERE id = ? AND status = 'COMPLETING'",
                id) == 1;
    }

    /** Sagas that have sat in COMPLETING for longer than {@code olderThan}, oldest first. */
    public List<TransactionRow> findStuckCompleting(Duration olderThan, int limit) {
        return jdbc.query(
                "SELECT id, station_id, status, item_count, running_total, started_at, completed_at "
                        + "FROM transactions "
                        + "WHERE status = 'COMPLETING' AND completing_since < now() - make_interval(secs => ?) "
                        + "ORDER BY completing_since LIMIT ?",
                TX_MAPPER, (double) olderThan.toMillis() / 1000.0, limit);
    }

    /** Wipes all run state, outbox included. Part of a reset; see CheckoutResetService. */
    public void truncateAll() {
        jdbc.execute("TRUNCATE transaction_items, transactions, scan_events RESTART IDENTITY CASCADE");
    }

    /**
     * Basket lines, ordered by SKU.
     *
     * In week 4 this ordering was checkout's deadlock-avoidance mechanism,
     * because checkout took the inventory row locks. inventory-service takes
     * them now and enforces the order itself; sorting here just keeps the
     * receipt lines in the same order as before.
     */
    public List<TransactionLine> findLinesOrderedBySku(UUID transactionId) {
        return jdbc.query(
                "SELECT sku, quantity, unit_price FROM transaction_items "
                        + "WHERE transaction_id = ? ORDER BY sku",
                LINE_MAPPER, transactionId);
    }
}
