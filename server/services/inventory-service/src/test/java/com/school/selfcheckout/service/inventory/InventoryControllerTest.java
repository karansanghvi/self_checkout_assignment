package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.Dtos.LowStockResponse;
import com.school.selfcheckout.web.GlobalExceptionHandler;
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

class InventoryControllerTest {

    private final LowStockService lowStockService = mock(LowStockService.class);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new InventoryController(lowStockService))
            .setControllerAdvice(new GlobalExceptionHandler())
            .build();

    @Test
    void lowStockPassesAbsentThresholdThroughAsNull() throws Exception {
        when(lowStockService.lowStock(any())).thenReturn(new LowStockResponse(50, Instant.EPOCH, List.of()));

        mvc.perform(get("/inventory/low-stock")).andExpect(status().isOk());

        ArgumentCaptor<Integer> threshold = ArgumentCaptor.forClass(Integer.class);
        verify(lowStockService).lowStock(threshold.capture());
        assertThat(threshold.getValue()).isNull();
    }
}
