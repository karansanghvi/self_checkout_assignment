package com.school.selfcheckout.data.analytics;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Read side of the scan feed. checkout-service writes {@code scan_events}
 * (see {@code TransactionRepository#recordScan}); analytics-service reads it
 * here. This table is the entire interface between the two services.
 */
@Repository
public class ScanEventRepository {

    private final JdbcTemplate jdbc;

    public ScanEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Up to {@code limit} events with {@code seq > afterSeq}, in seq order,
     * together with the current epoch.
     *
     * The LEFT JOIN LATERAL keeps the epoch row even when there are no new
     * events, so every poll learns whether a reset has happened.
     */
    public ScanBatch poll(long afterSeq, int limit) {
        List<ScanEvent> events = new ArrayList<>();
        long[] epoch = {0L};
        jdbc.query(
                """
                SELECT e.epoch, s.seq, s.sku
                  FROM system_epoch e
                  LEFT JOIN LATERAL (
                      SELECT seq, sku
                        FROM scan_events
                       WHERE seq > ?
                       ORDER BY seq
                       LIMIT ?
                  ) s ON true
                 WHERE e.id = 1
                """,
                rs -> {
                    epoch[0] = rs.getLong("epoch");
                    String sku = rs.getString("sku");
                    if (sku != null) {
                        events.add(new ScanEvent(rs.getLong("seq"), sku));
                    }
                },
                afterSeq, limit);
        return new ScanBatch(epoch[0], events);
    }
}
