package com.school.selfcheckout.repository;

import com.school.selfcheckout.domain.PopularEntry;
import com.school.selfcheckout.domain.PopularWindow;
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

    /** Persists one hop of the window: the header plus its ranked entries. */
    @Transactional
    public void saveWindow(long windowStart, long windowEnd, int windowSize, int slideInterval,
                           List<PopularEntry> entries) {
        Long windowId = jdbc.queryForObject(
                "INSERT INTO popular_item_windows "
                        + "(window_start, window_end, window_size, slide_interval) "
                        + "VALUES (?, ?, ?, ?) RETURNING id",
                Long.class, windowStart, windowEnd, windowSize, slideInterval);

        if (windowId == null || entries.isEmpty()) {
            return;
        }

        jdbc.batchUpdate(
                "INSERT INTO popular_item_entries (window_id, rank, sku, scan_count) VALUES (?, ?, ?, ?)",
                entries, entries.size(),
                (ps, entry) -> {
                    ps.setLong(1, windowId);
                    ps.setInt(2, entry.rank());
                    ps.setString(3, entry.sku());
                    ps.setInt(4, entry.scanCount());
                });
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
}
