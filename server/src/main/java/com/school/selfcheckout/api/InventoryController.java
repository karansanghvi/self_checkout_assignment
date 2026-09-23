package com.school.selfcheckout.api;

import com.school.selfcheckout.analytics.LowStockService;
import com.school.selfcheckout.contract.Dtos.LowStockResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InventoryController {

    private final LowStockService lowStockService;

    public InventoryController(LowStockService lowStockService) {
        this.lowStockService = lowStockService;
    }

    @GetMapping("/inventory/low-stock")
    public LowStockResponse lowStock(@RequestParam(required = false) Integer threshold) {
        return lowStockService.lowStock(threshold);
    }
}
