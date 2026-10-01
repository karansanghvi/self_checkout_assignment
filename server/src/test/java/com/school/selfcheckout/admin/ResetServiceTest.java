package com.school.selfcheckout.admin;

import com.school.selfcheckout.analytics.PopularityService;
import com.school.selfcheckout.catalog.CatalogService;
import com.school.selfcheckout.config.AppProperties;
import com.school.selfcheckout.repository.ResetRepository;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Covers the default-resolution that moved out of {@code AdminController},
 * where the 2000/10000 fallbacks used to be hardcoded alongside -- and able to
 * drift from -- {@link AppProperties}.
 */
class ResetServiceTest {

    private final ResetRepository resetRepository = mock(ResetRepository.class);
    private final CatalogService catalogService = mock(CatalogService.class);
    private final PopularityService popularityService = mock(PopularityService.class);

    private ResetService newService(int catalogSize, int stockPerItem) {
        AppProperties properties = new AppProperties();
        properties.setCatalogSize(catalogSize);
        properties.setStockPerItem(stockPerItem);
        return new ResetService(resetRepository, catalogService, popularityService, properties);
    }

    @Test
    void nullOverridesFallBackToConfiguration() {
        newService(1234, 99).reset(null, null);

        verify(resetRepository).reseed(1234, 99);
    }

    @Test
    void explicitOverridesWin() {
        newService(1234, 99).reset(7, 8);

        verify(resetRepository).reseed(7, 8);
    }

    @Test
    void overridesResolveIndependently() {
        newService(1234, 99).reset(7, null);

        verify(resetRepository).reseed(7, 99);
    }

    @Test
    void noArgResetUsesConfiguration() {
        newService(2000, 10000).reset();

        verify(resetRepository).reseed(2000, 10000);
    }

    @Test
    void inMemoryStateIsResyncedAfterReseed() {
        Map<String, Object> result = newService(2000, 10000).reset(null, null);

        // The popularity pipeline is drained before the reseed, so no stale
        // snapshot lands after the tables are emptied; the catalog cache is
        // reloaded after it, so it describes the new data.
        InOrder order = inOrder(popularityService, resetRepository, catalogService);
        order.verify(popularityService).reset();
        order.verify(resetRepository).reseed(2000, 10000);
        order.verify(catalogService).reload();

        assertThat(result).containsEntry("status", "ok")
                .containsEntry("catalogSize", 2000)
                .containsEntry("stockPerItem", 10000);
    }
}
