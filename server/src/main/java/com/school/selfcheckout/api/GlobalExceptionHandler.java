package com.school.selfcheckout.api;

import com.school.selfcheckout.contract.Dtos.ApiError;
import com.school.selfcheckout.domain.CheckoutException;
import com.school.selfcheckout.domain.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Spring Boot's default error body ({@code timestamp/status/error/path}) does
 * not match the contract's {@code {error, message}} shape, so every failure
 * route has to be funnelled through here.
 *
 * This class is also the only place that maps a business {@link ErrorCode} to
 * an HTTP status. The layers below report what went wrong; deciding that
 * "not open" means 409 is a protocol concern and stays in the API layer.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(CheckoutException.class)
    public ResponseEntity<ApiError> handleCheckoutException(CheckoutException ex) {
        return ResponseEntity.status(statusFor(ex.getErrorCode()))
                .body(new ApiError(ex.getErrorCode().name(), ex.getMessage()));
    }

    /** The error-code to status table. Exhaustive switch, so a new code will not compile until mapped. */
    private static HttpStatus statusFor(ErrorCode code) {
        return switch (code) {
            case NOT_FOUND, UNKNOWN_SKU -> HttpStatus.NOT_FOUND;
            case TRANSACTION_NOT_OPEN, EMPTY_BASKET -> HttpStatus.CONFLICT;
            case INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
        };
    }

    /** A missing or blank {@code stationId}/{@code sku} must be a 400, not a 500. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException ex) {
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(fe -> fe.getField() + " " + fe.getDefaultMessage())
                .orElse("Request body failed validation");
        return ResponseEntity.badRequest()
                .body(new ApiError(ErrorCode.INVALID_REQUEST.name(), detail));
    }

    /** Malformed or absent JSON body where one is required. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest().body(new ApiError(
                ErrorCode.INVALID_REQUEST.name(), "Malformed or missing JSON request body"));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiError> handleNoResource(NoResourceFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ApiError(
                ErrorCode.NOT_FOUND.name(), "No such route: " + ex.getResourcePath()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiError("INTERNAL_ERROR", String.valueOf(ex.getMessage())));
    }
}
