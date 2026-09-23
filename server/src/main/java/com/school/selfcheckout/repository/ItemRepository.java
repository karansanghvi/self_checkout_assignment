package com.school.selfcheckout.repository;

import com.school.selfcheckout.domain.Item;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ItemRepository {

    private static final RowMapper<Item> ITEM_MAPPER = (rs, rowNum) ->
            new Item(rs.getString("sku"), rs.getString("name"), rs.getBigDecimal("price"));

    private final JdbcTemplate jdbc;

    public ItemRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * ORDER BY sku is load-bearing, not cosmetic. The load client's
     * ItemSampler treats a item's *position* in this list as its Zipf rank, so
     * an unstable order silently destroys cross-run comparability of the
     * popular-items results.
     */
    public List<Item> findAllOrderedBySku() {
        return jdbc.query("SELECT sku, name, price FROM items ORDER BY sku", ITEM_MAPPER);
    }
}
