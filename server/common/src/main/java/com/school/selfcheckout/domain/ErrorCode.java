package com.school.selfcheckout.domain;

/**
 * The business vocabulary of failure.
 *
 * Constant names are the exact {@code error} values the contract puts on the
 * wire, so {@link #name()} is the serialised form -- do not rename without
 * re-reading the spec.
 *
 * Deliberately carries no HTTP status. Choosing 404 versus 409 is a protocol
 * decision and belongs at the HTTP edge; see
 * {@code GlobalExceptionHandler#statusFor}. That split is what lets the
 * services say *what* went wrong without knowing
 * they are being served over HTTP.
 */
public enum ErrorCode {
    NOT_FOUND,
    UNKNOWN_SKU,
    TRANSACTION_NOT_OPEN,
    EMPTY_BASKET,
    INVALID_REQUEST,
    /**
     * Another service this request depends on could not be reached, or
     * refused the request. Not in the OpenAPI contract: it exists because a
     * microservice can fail in a way a single process cannot.
     */
    DEPENDENCY_FAILED
}
