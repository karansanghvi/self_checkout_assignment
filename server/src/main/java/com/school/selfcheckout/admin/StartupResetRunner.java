package com.school.selfcheckout.admin;

import com.school.selfcheckout.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Optional wipe-and-reseed at boot, enabled with {@code --app.reset-on-startup=true}.
 *
 * ApplicationRunner beans execute before ApplicationReadyEvent is published,
 * so this finishes before CatalogService warms its cache.
 */
@Component
public class StartupResetRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StartupResetRunner.class);

    private final AppProperties properties;
    private final ResetService resetService;

    public StartupResetRunner(AppProperties properties, ResetService resetService) {
        this.properties = properties;
        this.resetService = resetService;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isResetOnStartup()) {
            return;
        }
        log.info("app.reset-on-startup=true -- wiping and reseeding");
        resetService.reset();
    }
}
