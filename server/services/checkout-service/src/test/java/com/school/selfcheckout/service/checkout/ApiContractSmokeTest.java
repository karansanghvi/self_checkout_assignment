package com.school.selfcheckout.service.checkout;

import com.school.selfcheckout.contract.Dtos.TransactionResponse;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.web.GlobalExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Wire-contract smoke tests for checkout-service's HTTP edge.
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

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(
                        new TransactionController(transactionService))
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

    @Test
    void anUnreachableDependencyIs503() throws Exception {
        when(transactionService.complete(anyString()))
                .thenThrow(CheckoutException.dependencyFailed("inventory-service is unavailable"));

        mvc.perform(post("/transactions/1a2b/complete"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("DEPENDENCY_FAILED"));
    }
}
