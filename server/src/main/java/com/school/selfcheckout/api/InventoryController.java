package com.school.selfcheckout.api;

import com.school.selfcheckout.api.dto.Dtos.LowStockResponse;
import com.school.selfcheckout.service.InventoryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping("/inventory/low-stock")
    public LowStockResponse lowStock(@RequestParam(required = false) Integer threshold) {
        return inventoryService.lowStock(threshold);
    }
}
