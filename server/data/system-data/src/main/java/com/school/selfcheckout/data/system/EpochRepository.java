package com.school.selfcheckout.data.system;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Reads the reset generation counter in {@code system_epoch}. */
@Repository
public class EpochRepository {

    private final JdbcTemplate jdbc;

    public EpochRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long currentEpoch() {
        Long epoch = jdbc.queryForObject("SELECT epoch FROM system_epoch WHERE id = 1", Long.class);
        return epoch == null ? 0L : epoch;
    }
}
