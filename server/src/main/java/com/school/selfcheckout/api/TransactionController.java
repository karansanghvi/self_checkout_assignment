package com.school.selfcheckout.api;

import com.school.selfcheckout.api.dto.Dtos.Receipt;
import com.school.selfcheckout.api.dto.Dtos.ScanItemRequest;
import com.school.selfcheckout.api.dto.Dtos.ScanResult;
import com.school.selfcheckout.api.dto.Dtos.StartTransactionRequest;
import com.school.selfcheckout.api.dto.Dtos.TransactionResponse;
import com.school.selfcheckout.service.TransactionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    /** Must be 201, not 200 -- the load client treats any other status as a failure. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public TransactionResponse start(@Valid @RequestBody StartTransactionRequest request) {
        return transactionService.start(request.stationId());
    }

    @PostMapping("/{transactionId}/items")
    public ScanResult scan(@PathVariable String transactionId,
                           @Valid @RequestBody ScanItemRequest request) {
        return transactionService.scan(transactionId, request.sku());
    }

    /**
     * The load client posts a literal {@code {}} here, so no request body is
     * declared -- binding one would make an empty object a 400.
     */
    @PostMapping("/{transactionId}/complete")
    public Receipt complete(@PathVariable String transactionId) {
        return transactionService.complete(transactionId);
    }

    @GetMapping("/{transactionId}")
    public TransactionResponse get(@PathVariable String transactionId) {
        return transactionService.get(transactionId);
    }
}
