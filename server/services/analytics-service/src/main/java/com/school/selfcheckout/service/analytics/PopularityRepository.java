package com.school.selfcheckout.service.analytics;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public class PopularityRepository {

    private static final RowMapper<PopularWindow> WINDOW_MAPPER = (rs, rowNum) -> new PopularWindow(
            rs.getLong("id"),
            rs.getLong("window_start"),
            rs.getLong("window_end"),
            rs.getInt("window_size"),
            rs.getInt("slide_interval"),
            rs.getTimestamp("computed_at").toInstant());

    private final JdbcTemplate jdbc;

    public PopularityRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Persists one hop of the window -- the header plus its ranked entries --
     * provided the database is still at {@code epoch}.
     *
     * The share lock on the state row orders this against a reset:
     * {@link #resetTo} updates that row before it truncates anything. If the
     * reset got there first, this sees the new epoch and writes nothing; if
     * this got there first, the reset waits, then truncates what was written.
     * Either way no window from a previous run survives a reset.
     *
     * A window whose {@code window_end} is already stored is skipped, which
     * makes replaying the topic after a restart harmless.
     *
     * @return false if the window was discarded because a reset intervened
     */
    @Transactional
    public boolean saveWindow(long epoch, long windowStart, long windowEnd, int windowSize, int slideInterval,
                              List<PopularEntry> entries) {
        Long current = jdbc.queryForObject(
                "SELECT epoch FROM analytics_state WHERE id = 1 FOR SHARE", Long.class);
        if (current == null || current != epoch) {
            return false;
        }

        List<Long> inserted = jdbc.queryForList(
                "INSERT INTO popular_item_windows "
                        + "(window_start, window_end, window_size, slide_interval) "
                        + "VALUES (?, ?, ?, ?) "
                        + "ON CONFLICT (window_end) DO NOTHING RETURNING id",
                Long.class, windowStart, windowEnd, windowSize, slideInterval);

        if (inserted.isEmpty() || entries.isEmpty()) {
            return true;
        }
        long windowId = inserted.get(0);

        jdbc.batchUpdate(
                "INSERT INTO popular_item_entries (window_id, rank, sku, scan_count) VALUES (?, ?, ?, ?)",
                entries, entries.size(),
                (ps, entry) -> {
                    ps.setLong(1, windowId);
                    ps.setInt(2, entry.rank());
                    ps.setString(3, entry.sku());
                    ps.setInt(4, entry.scanCount());
                });
        return true;
    }

    /** @return the most recently computed window, or {@code null} if none has been computed yet. */
    public PopularWindow findLatestWindow() {
        try {
            return jdbc.queryForObject(
                    "SELECT id, window_start, window_end, window_size, slide_interval, computed_at "
                            + "FROM popular_item_windows ORDER BY id DESC LIMIT 1",
                    WINDOW_MAPPER);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<PopularEntry> findEntries(long windowId, int limit) {
        return jdbc.query(
                "SELECT rank, sku, scan_count FROM popular_item_entries "
                        + "WHERE window_id = ? ORDER BY rank ASC LIMIT ?",
                (rs, rowNum) -> new PopularEntry(rs.getInt("rank"), rs.getString("sku"), rs.getInt("scan_count")),
                windowId, limit);
    }

    public AnalyticsState loadState() {
        return jdbc.queryForObject(
                "SELECT epoch, start_offset FROM analytics_state WHERE id = 1",
                (rs, rowNum) -> new AnalyticsState(rs.getLong("epoch"), rs.getLong("start_offset")));
    }

    /**
     * Starts {@code epoch}: records where in the topic its events begin, and
     * empties the persisted windows. The state row is updated first, so its
     * row lock is held before anything is truncated -- see {@link #saveWindow}.
     */
    @Transactional
    public void resetTo(long epoch, long startOffset) {
        jdbc.update("UPDATE analytics_state SET epoch = ?, start_offset = ?, reset_at = now() WHERE id = 1",
                epoch, startOffset);
        jdbc.execute("TRUNCATE popular_item_entries, popular_item_windows RESTART IDENTITY");
    }
}
