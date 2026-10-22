package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import com.school.selfcheckout.contract.InternalDtos.StockCommitRequest;
import com.school.selfcheckout.contract.InternalDtos.StockCommitResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Service-to-service endpoints. The gateway does not route {@code /internal/**}. */
@RestController
public class InternalController {

    private final StockCommitService stockCommitService;
    private final InventoryResetService resetService;

    public InternalController(StockCommitService stockCommitService, InventoryResetService resetService) {
        this.stockCommitService = stockCommitService;
        this.resetService = resetService;
    }

    /** Step 2 of checkout-service's completion saga. Safe to retry: see {@link StockCommitService}. */
    @PostMapping("/internal/stock-commits")
    public StockCommitResult commit(@RequestBody StockCommitRequest request) {
        return stockCommitService.commit(request);
    }

    @PostMapping("/internal/reset")
    public ResetAck reset(@RequestBody ResetCommand command) {
        return resetService.reset(command);
    }
}
