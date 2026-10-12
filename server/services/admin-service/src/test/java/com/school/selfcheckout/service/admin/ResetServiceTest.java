package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.data.system.ResetRepository;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the default-resolution that moved out of {@code AdminController},
 * where the 2000/10000 fallbacks used to be hardcoded alongside -- and able to
 * drift from -- the configuration.
 */
class ResetServiceTest {

    private final ResetRepository resetRepository = mock(ResetRepository.class);

    private ResetService newService(int catalogSize, int stockPerItem) {
        return new ResetService(resetRepository, catalogSize, stockPerItem);
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

    /**
     * The monolith's version of this test checked that the reset drained the
     * pipeline and reloaded the catalog cache in order. Those are other
     * services' state now; what admin-service owes them is the new epoch.
     */
    @Test
    void reportsTheNewEpoch() {
        when(resetRepository.reseed(2000, 10000)).thenReturn(42L);

        Map<String, Object> result = newService(2000, 10000).reset(null, null);

        assertThat(result).containsEntry("status", "ok")
                .containsEntry("catalogSize", 2000)
                .containsEntry("stockPerItem", 10000)
                .containsEntry("epoch", 42L);
    }
}
