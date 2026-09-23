package com.school.selfcheckout.api;

import com.school.selfcheckout.api.dto.Dtos.PopularItemsResponse;
import com.school.selfcheckout.service.PopularityService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AnalyticsController {

    private final PopularityService popularityService;

    public AnalyticsController(PopularityService popularityService) {
        this.popularityService = popularityService;
    }

    @GetMapping("/analytics/popular-items")
    public PopularItemsResponse popularItems(
            @RequestParam(required = false, defaultValue = "10") Integer limit) {
        return popularityService.popularItems(limit);
    }
}
