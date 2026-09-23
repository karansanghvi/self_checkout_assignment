package com.school.selfcheckout.api;

import com.school.selfcheckout.service.ResetService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Not part of the OpenAPI contract. Extra routes are harmless -- the load
 * client only ever calls the seven contract endpoints -- and this makes
 * reinitialising between runs a single HTTP call.
 */
@RestController
public class AdminController {

    private final ResetService resetService;

    public AdminController(ResetService resetService) {
        this.resetService = resetService;
    }

    @PostMapping("/admin/reset")
    public Map<String, Object> reset(
            @RequestParam(required = false) Integer catalogSize,
            @RequestParam(required = false) Integer stockPerItem) {
        if (catalogSize == null && stockPerItem == null) {
            return resetService.reset();
        }
        return resetService.reset(
                catalogSize != null ? catalogSize : 2000,
                stockPerItem != null ? stockPerItem : 10000);
    }
}
