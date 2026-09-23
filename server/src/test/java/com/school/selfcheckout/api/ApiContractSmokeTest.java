package com.school.selfcheckout.api;

import com.school.selfcheckout.admin.ResetService;
import com.school.selfcheckout.analytics.LowStockService;
import com.school.selfcheckout.analytics.PopularityService;
import com.school.selfcheckout.catalog.CatalogService;
import com.school.selfcheckout.contract.Dtos.CatalogResponse;
import com.school.selfcheckout.contract.Dtos.LowStockResponse;
import com.school.selfcheckout.contract.Dtos.PopularItemsResponse;
import com.school.selfcheckout.contract.Dtos.TransactionResponse;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.transactions.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Wire-contract smoke tests for the API layer.
 *
 * The services are mocked, so this needs no database and no Spring context --
 * it exercises exactly the seam that the refactor changed: the translation of a
 * business {@code ErrorCode} into an HTTP status and error body, which used to
 * be decided inside {@code TransactionService} via {@code ApiException}.
 *
 * Every expected status and error string here was read off the pre-refactor
 * {@code ApiException} factories.
 */
class ApiContractSmokeTest {

    private final TransactionService transactionService = mock(TransactionService.class);
    private final PopularityService popularityService = mock(PopularityService.class);
    private final LowStockService lowStockService = mock(LowStockService.class);
    private final CatalogService catalogService = mock(CatalogService.class);
    private final ResetService resetService = mock(ResetService.class);

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(
                        new TransactionController(transactionService),
                        new AnalyticsController(popularityService),
                        new InventoryController(lowStockService),
                        new CatalogController(catalogService),
                        new AdminController(resetService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    // --- status codes ----------------------------------------------------

    @Test
    void startTransactionReturns201() throws Exception {
        when(transactionService.start("station-1")).thenReturn(new TransactionResponse(
                "1a2b", "station-1", "OPEN", 0, new BigDecimal("0.00"), Instant.EPOCH));

        mvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stationId\":\"station-1\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.transactionId").value("1a2b"))
                .andExpect(jsonPath("$.status").value("OPEN"));
    }

    @Test
    void blankStationIdIsRejectedAsInvalidRequest() throws Exception {
        mvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stationId\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("INVALID_REQUEST"));
    }

    @Test
    void malformedBodyIsInvalidRequest() throws Exception {
        mvc.perform(post("/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").value("Malformed or missing JSON request body"));
    }

    // --- ErrorCode to HTTP status mapping --------------------------------

    @Test
    void unknownSkuIs404() throws Exception {
        when(transactionService.scan(anyString(), anyString()))
                .thenThrow(CheckoutException.unknownSku("SKU-999999"));

        mvc.perform(post("/transactions/1a2b/items")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sku\":\"SKU-999999\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("UNKNOWN_SKU"))
                .andExpect(jsonPath("$.message").value("No such SKU: SKU-999999"));
    }

    @Test
    void missingTransactionIs404() throws Exception {
        when(transactionService.get(anyString()))
                .thenThrow(CheckoutException.transactionNotFound("nope"));

        mvc.perform(get("/transactions/nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("NOT_FOUND"))
                .andExpect(jsonPath("$.message").value("No such transaction: nope"));
    }

    @Test
    void alreadyCompletedTransactionIs409() throws Exception {
        when(transactionService.complete(anyString()))
                .thenThrow(CheckoutException.transactionNotOpen("1a2b", "COMPLETED"));

        mvc.perform(post("/transactions/1a2b/complete"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("TRANSACTION_NOT_OPEN"))
                .andExpect(jsonPath("$.message").value("Transaction 1a2b is COMPLETED."));
    }

    @Test
    void emptyBasketIs409() throws Exception {
        when(transactionService.complete(anyString()))
                .thenThrow(CheckoutException.emptyBasket("1a2b"));

        mvc.perform(post("/transactions/1a2b/complete"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("EMPTY_BASKET"))
                .andExpect(jsonPath("$.message")
                        .value("Cannot complete transaction 1a2b with an empty basket."));
    }

    @Test
    void completeAcceptsAnEmptyJsonObjectBody() throws Exception {
        when(transactionService.complete("1a2b")).thenThrow(CheckoutException.emptyBasket("1a2b"));

        // The load client posts a literal {} here; it must not become a 400.
        mvc.perform(post("/transactions/1a2b/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isConflict());
    }

    // --- optional query parameters reach the business layer unresolved ---

    @Test
    void popularItemsPassesAbsentLimitThroughAsNull() throws Exception {
        when(popularityService.popularItems(any())).thenReturn(
                new PopularItemsResponse(1000, 500, 0, 0, Instant.EPOCH, List.of()));

        mvc.perform(get("/analytics/popular-items")).andExpect(status().isOk());

        ArgumentCaptor<Integer> limit = ArgumentCaptor.forClass(Integer.class);
        verify(popularityService).popularItems(limit.capture());
        assertThat(limit.getValue())
                .as("the analytics layer owns the default, so the controller must not supply one")
                .isNull();
    }

    @Test
    void popularItemsForwardsAnExplicitLimit() throws Exception {
        when(popularityService.popularItems(any())).thenReturn(
                new PopularItemsResponse(1000, 500, 0, 0, Instant.EPOCH, List.of()));

        mvc.perform(get("/analytics/popular-items").param("limit", "5"))
                .andExpect(status().isOk());

        verify(popularityService).popularItems(5);
    }

    @Test
    void lowStockPassesAbsentThresholdThroughAsNull() throws Exception {
        when(lowStockService.lowStock(any()))
                .thenReturn(new LowStockResponse(50, Instant.EPOCH, List.of()));

        mvc.perform(get("/inventory/low-stock")).andExpect(status().isOk());

        ArgumentCaptor<Integer> threshold = ArgumentCaptor.forClass(Integer.class);
        verify(lowStockService).lowStock(threshold.capture());
        assertThat(threshold.getValue()).isNull();
    }

    @Test
    void resetPassesAbsentSizesThroughAsNull() throws Exception {
        when(resetService.reset(any(), any())).thenReturn(Map.of("status", "ok"));

        mvc.perform(post("/admin/reset")).andExpect(status().isOk());

        // Previously the controller substituted hardcoded 2000/10000 here.
        verify(resetService).reset(null, null);
    }

    @Test
    void itemsIsServedFromTheCatalogLayer() throws Exception {
        when(catalogService.catalogResponse()).thenReturn(new CatalogResponse(List.of()));

        mvc.perform(get("/items"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items").isArray());
    }
}
