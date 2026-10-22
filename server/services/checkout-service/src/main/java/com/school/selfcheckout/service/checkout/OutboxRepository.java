package com.school.selfcheckout.service.checkout;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * The outbox ({@code scan_events}) and the relay's position in it
 * ({@code relay_state}), both in checkout_db.
 */
@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbc;
    private final TransactionRepository transactionRepository;

    public OutboxRepository(JdbcTemplate jdbc, TransactionRepository transactionRepository) {
        this.jdbc = jdbc;
        this.transactionRepository = transactionRepository;
    }

    /** Up to {@code limit} events with {@code seq > afterSeq}, in seq order. */
    public List<OutboxEvent> poll(long afterSeq, int limit) {
        return jdbc.query(
                "SELECT seq, sku FROM scan_events WHERE seq > ? ORDER BY seq LIMIT ?",
                (rs, rowNum) -> new OutboxEvent(rs.getLong("seq"), rs.getString("sku")),
                afterSeq, limit);
    }

    public RelayState loadState() {
        return jdbc.queryForObject(
                "SELECT epoch, published_seq FROM relay_state WHERE id = 1",
                (rs, rowNum) -> new RelayState(rs.getLong("epoch"), rs.getLong("published_seq")));
    }

    public void savePublishedSeq(long seq) {
        jdbc.update("UPDATE relay_state SET published_seq = ? WHERE id = 1", seq);
    }

    /**
     * Wipes all run state and starts {@code epoch}, in one transaction, so a
     * crash cannot leave an emptied outbox paired with an old epoch.
     */
    @Transactional
    public void resetTo(long epoch) {
        transactionRepository.truncateAll();
        jdbc.update("UPDATE relay_state SET epoch = ?, published_seq = 0 WHERE id = 1", epoch);
    }
}
