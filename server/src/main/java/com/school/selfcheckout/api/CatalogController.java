package com.school.selfcheckout.api;

import com.school.selfcheckout.catalog.CatalogService;
import com.school.selfcheckout.contract.Dtos.CatalogResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CatalogController {

    private final CatalogService catalogService;

    public CatalogController(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @GetMapping("/items")
    public CatalogResponse listItems() {
        return catalogService.catalogResponse();
    }
}
