package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.contract.Dtos.CatalogItem;
import com.school.selfcheckout.contract.Dtos.CatalogResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CatalogController {

    private final CatalogCache catalog;

    public CatalogController(CatalogCache catalog) {
        this.catalog = catalog;
    }

    /** Order comes from the cache, which holds items ORDER BY sku -- the load client depends on it. */
    @GetMapping("/items")
    public CatalogResponse listItems() {
        return new CatalogResponse(catalog.allOrderedBySku().stream()
                .map(i -> new CatalogItem(i.sku(), i.name(), i.price()))
                .toList());
    }
}
