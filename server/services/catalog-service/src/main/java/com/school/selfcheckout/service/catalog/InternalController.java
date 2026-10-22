package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Service-to-service endpoints. The gateway does not route {@code /internal/**}. */
@RestController
public class InternalController {

    private final CatalogResetService resetService;

    public InternalController(CatalogResetService resetService) {
        this.resetService = resetService;
    }

    @PostMapping("/internal/reset")
    public ResetAck reset(@RequestBody ResetCommand command) {
        return resetService.reset(command);
    }
}
