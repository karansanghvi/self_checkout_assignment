package com.school.selfcheckout.domain;

/**
 * A business rule was violated.
 *
 * Replaces the earlier {@code api.ApiException}, which carried an
 * {@code HttpStatus} and therefore forced every service that threw it to
 * depend on the API layer. The factories below preserve the previous messages
 * verbatim, so the bytes on the wire are unchanged.
 */
public class CheckoutException extends RuntimeException {

    private final ErrorCode errorCode;

    public CheckoutException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static CheckoutException transactionNotFound(String transactionId) {
        return new CheckoutException(ErrorCode.NOT_FOUND, "No such transaction: " + transactionId);
    }

    public static CheckoutException unknownSku(String sku) {
        return new CheckoutException(ErrorCode.UNKNOWN_SKU, "No such SKU: " + sku);
    }

    public static CheckoutException transactionNotOpen(String transactionId, String status) {
        return new CheckoutException(ErrorCode.TRANSACTION_NOT_OPEN,
                "Transaction " + transactionId + " is " + status + ".");
    }

    public static CheckoutException emptyBasket(String transactionId) {
        return new CheckoutException(ErrorCode.EMPTY_BASKET,
                "Cannot complete transaction " + transactionId + " with an empty basket.");
    }

    public static CheckoutException invalidRequest(String message) {
        return new CheckoutException(ErrorCode.INVALID_REQUEST, message);
    }
}
