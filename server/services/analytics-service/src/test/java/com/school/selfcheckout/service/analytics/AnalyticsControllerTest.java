package com.school.selfcheckout.service.analytics;

import com.school.selfcheckout.contract.Dtos.PopularItemsResponse;
import com.school.selfcheckout.web.GlobalExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AnalyticsControllerTest {

    private final PopularityService popularityService = mock(PopularityService.class);
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new AnalyticsController(popularityService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
        when(popularityService.popularItems(any())).thenReturn(
                new PopularItemsResponse(1000, 500, 0, 0, Instant.EPOCH, List.of()));
    }

    @Test
    void popularItemsPassesAbsentLimitThroughAsNull() throws Exception {
        mvc.perform(get("/analytics/popular-items")).andExpect(status().isOk());

        ArgumentCaptor<Integer> limit = ArgumentCaptor.forClass(Integer.class);
        verify(popularityService).popularItems(limit.capture());
        assertThat(limit.getValue())
                .as("PopularityService owns the default, so the controller must not supply one")
                .isNull();
    }

    @Test
    void popularItemsForwardsAnExplicitLimit() throws Exception {
        mvc.perform(get("/analytics/popular-items").param("limit", "5")).andExpect(status().isOk());

        verify(popularityService).popularItems(5);
    }
}
