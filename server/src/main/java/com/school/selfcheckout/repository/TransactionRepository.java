package com.school.selfcheckout.repository;

import com.school.selfcheckout.domain.TransactionRow;
import com.school.selfcheckout.domain.TransactionLine;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

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
     * it is deliberately a single round trip. The data-modifying CTE upserts
     * the per-SKU line while the outer UPDATE bumps the denormalized counters
     * on the transaction row; Postgres guarantees a data-modifying WITH clause
     * runs exactly once and to completion, regardless of whether the primary
     * query reads its output.
     *
     * The EXISTS guard keeps the line insert and the counter update consistent:
     * if the transaction is missing or no longer OPEN, neither happens.
     *
     * No locking is needed here. A transaction row is only ever touched by the
     * one station that owns it, so there is no contention to protect against --
     * unlike the inventory rows, which every station fights over.
     *
     * @return the new {@code (itemCount, runningTotal)}, or {@code null} if the
     *         transaction was missing or not OPEN.
     */
    public ScanTotals recordScan(UUID transactionId, String sku, BigDecimal unitPrice) {
        List<ScanTotals> result = jdbc.query(
                """
                WITH upsert AS (
                    INSERT INTO transaction_items (transaction_id, sku, quantity, unit_price)
                    SELECT CAST(? AS uuid), CAST(? AS text), 1, CAST(? AS numeric)
                     WHERE EXISTS (SELECT 1 FROM transactions WHERE id = ? AND status = 'OPEN')
                    ON CONFLICT (transaction_id, sku)
                    DO UPDATE SET quantity = transaction_items.quantity + 1
                    RETURNING 1
                )
                UPDATE transactions
                   SET item_count    = item_count + 1,
                       running_total = running_total + ?
                 WHERE id = ? AND status = 'OPEN'
                RETURNING item_count, running_total
                """,
                (rs, rowNum) -> new ScanTotals(rs.getInt("item_count"), rs.getBigDecimal("running_total")),
                transactionId, sku, unitPrice, transactionId, unitPrice, transactionId);
        return result.isEmpty() ? null : result.get(0);
    }

    public record ScanTotals(int itemCount, BigDecimal runningTotal) {
    }

    /**
     * Atomically claims the transaction for completion.
     *
     * The {@code AND status = 'OPEN'} predicate is what makes concurrent
     * double-completion safe: two simultaneous /complete calls both try this
     * UPDATE, the second blocks on the row lock, then re-evaluates the
     * predicate against the committed 'COMPLETED' row and matches nothing.
     *
     * @return the claimed transaction, or {@code null} if missing or not OPEN.
     */
    public TransactionRow claimForCompletion(UUID id) {
        List<TransactionRow> rows = jdbc.query(
                "UPDATE transactions SET status = 'COMPLETED', completed_at = now() "
                        + "WHERE id = ? AND status = 'OPEN' "
                        + "RETURNING id, station_id, status, item_count, running_total, started_at, completed_at",
                TX_MAPPER, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Basket lines, ordered by SKU.
     *
     * The ordering is the deadlock-avoidance mechanism, not presentation. Two
     * concurrent baskets containing {A,B} and {B,A} would otherwise grab the
     * two inventory row locks in opposite orders and deadlock; Postgres would
     * detect the cycle and abort one, surfacing as a 500 to the load client.
     * Taking locks in a single global order (lexicographic SKU) makes the
     * cycle impossible to form.
     */
    public List<TransactionLine> findLinesOrderedBySku(UUID transactionId) {
        return jdbc.query(
                "SELECT sku, quantity, unit_price FROM transaction_items "
                        + "WHERE transaction_id = ? ORDER BY sku",
                LINE_MAPPER, transactionId);
    }
}
