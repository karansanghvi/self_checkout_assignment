package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.messaging.Topic;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
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
     *   3. append a {@code ScanRecorded} event to the outbox, which
     *      {@link OutboxRelay} publishes to Kafka for analytics-service.
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
     * unlike the inventory rows, which every completion updates.
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
                    INSERT INTO outbox (topic, event_type, payload)
                    SELECT CAST(? AS text), 'ScanRecorded', json_build_object('sku', CAST(? AS text))::text
                      FROM tx
                )
                SELECT item_count, running_total FROM tx
                """,
                (rs, rowNum) -> new ScanTotals(rs.getInt("item_count"), rs.getBigDecimal("running_total")),
                unitPrice, transactionId, sku, unitPrice, Topic.SCAN_EVENTS.topicName(), sku);
        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * Completes a transaction: OPEN -> COMPLETED, and a
     * {@code TransactionCompleted} event in the outbox, in one database
     * transaction. That is all /complete does now. In week 5 this was a
     * three-step saga with an HTTP call to inventory-service in the middle;
     * inventory now takes the stock out when it consumes the event.
     *
     * Two statements, in this order, inside one transaction:
     *
     *   1. The claim. {@code AND status = 'OPEN'} makes concurrent
     *      double-completion safe: the second UPDATE blocks on the row lock,
     *      then re-evaluates the predicate against the committed COMPLETED
     *      row and matches nothing. {@code item_count > 0} refuses an empty
     *      basket in the same statement.
     *   2. Read the basket lines and write the event from them, in one
     *      statement so both see the same snapshot. It runs after the claim,
     *      and so with a snapshot taken after it: every scan committed before
     *      the claim is included, and no scan can land after it, because
     *      recordScan requires OPEN. Folding both into one statement would
     *      read the lines from a snapshot taken <i>before</i> the claim had
     *      waited for the row lock, and could miss a concurrent last scan.
     *
     * @return the completed transaction and its lines, or {@code null} if it
     *         was missing, not OPEN, or empty
     */
    @Transactional
    public Completion complete(UUID id) {
        List<TransactionRow> claimed = jdbc.query(
                "UPDATE transactions SET status = 'COMPLETED', completed_at = now() "
                        + "WHERE id = ? AND status = 'OPEN' AND item_count > 0 "
                        + "RETURNING id, station_id, status, item_count, running_total, started_at, completed_at",
                TX_MAPPER, id);
        if (claimed.isEmpty()) {
            return null;
        }
        TransactionRow tx = claimed.get(0);

        List<TransactionLine> lines = jdbc.query(
                """
                WITH lines AS (
                    SELECT sku, quantity, unit_price FROM transaction_items WHERE transaction_id = ?
                ),
                event AS (
                    INSERT INTO outbox (topic, event_type, msg_key, payload)
                    SELECT CAST(? AS text), 'TransactionCompleted', CAST(? AS text),
                           json_build_object(
                               'transactionId', CAST(? AS text),
                               'stationId',     CAST(? AS text),
                               'lines', json_agg(json_build_object('sku', sku, 'quantity', quantity) ORDER BY sku)
                           )::text
                      FROM lines
                )
                SELECT sku, quantity, unit_price FROM lines ORDER BY sku
                """,
                LINE_MAPPER, id, Topic.TRANSACTION_COMPLETED.topicName(), id.toString(), id.toString(),
                tx.stationId());
        return new Completion(tx, lines);
    }

    /** Wipes all run state, outbox included. Part of a reset; see OutboxRelay#reset. */
    public void truncateAll() {
        jdbc.execute("TRUNCATE transaction_items, transactions, outbox RESTART IDENTITY CASCADE");
    }
}
