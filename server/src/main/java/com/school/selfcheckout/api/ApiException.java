package com.school.selfcheckout.api;

import org.springframework.http.HttpStatus;

/**
 * Carries the exact {@code error} code and HTTP status the contract expects.
 * Error codes match the reference MockServer so failures read the same way
 * across implementations.
 */
public class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final String errorCode;

    public ApiException(HttpStatus status, String errorCode, String message) {
        super(message);
        this.status = status;
        this.errorCode = errorCode;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public static ApiException transactionNotFound(String transactionId) {
        return new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND",
                "No such transaction: " + transactionId);
    }

    public static ApiException unknownSku(String sku) {
        return new ApiException(HttpStatus.NOT_FOUND, "UNKNOWN_SKU", "No such SKU: " + sku);
    }

    public static ApiException transactionNotOpen(String transactionId, String status) {
        return new ApiException(HttpStatus.CONFLICT, "TRANSACTION_NOT_OPEN",
                "Transaction " + transactionId + " is " + status + ".");
    }

    public static ApiException emptyBasket(String transactionId) {
        return new ApiException(HttpStatus.CONFLICT, "EMPTY_BASKET",
                "Cannot complete transaction " + transactionId + " with an empty basket.");
    }

    public static ApiException invalidRequest(String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", message);
    }
}
