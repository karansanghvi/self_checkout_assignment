package com.school.selfcheckout.service.catalog;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

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

    /**
     * Makes the catalog exactly {@code catalogSize} items, using the same
     * generator as the reference MockServer (and V2__seed_catalog.sql).
     * Items outside the requested size are dropped, so shrinking works as
     * well as growing. Records the reset's epoch in the same transaction.
     */
    @Transactional
    public void reseed(long epoch, int catalogSize, int stockPerItem) {
        jdbc.update("UPDATE catalog_state SET epoch = ?, stock_per_item = ? WHERE id = 1", epoch, stockPerItem);
        jdbc.update(
                "DELETE FROM items WHERE sku NOT IN "
                        + "(SELECT 'SKU-' || LPAD(i::text, 6, '0') FROM generate_series(1, ?) AS i)",
                catalogSize);
        jdbc.update("""
                INSERT INTO items (sku, name, price)
                SELECT 'SKU-' || LPAD(i::text, 6, '0'),
                       'Item ' || i,
                       ROUND((0.5 + (i % 47) * 0.35)::numeric, 2)
                  FROM generate_series(1, ?) AS i
                ON CONFLICT (sku) DO NOTHING
                """, catalogSize);
    }

    public CatalogState loadState() {
        return jdbc.queryForObject(
                "SELECT epoch, stock_per_item FROM catalog_state WHERE id = 1",
                (rs, rowNum) -> new CatalogState(rs.getLong("epoch"), rs.getInt("stock_per_item")));
    }
}
