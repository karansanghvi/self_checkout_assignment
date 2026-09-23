package com.school.selfcheckout.api;

import com.school.selfcheckout.analytics.PopularityService;
import com.school.selfcheckout.contract.Dtos.PopularItemsResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AnalyticsController {

    private final PopularityService popularityService;

    public AnalyticsController(PopularityService popularityService) {
        this.popularityService = popularityService;
    }

    /**
     * {@code limit} is passed through as-is, including null. Defaulting and
     * clamping belong to the analytics layer, which already owns them -- a
     * {@code defaultValue} here would duplicate that rule in two layers.
     */
    @GetMapping("/analytics/popular-items")
    public PopularItemsResponse popularItems(@RequestParam(required = false) Integer limit) {
        return popularityService.popularItems(limit);
    }
}
