package com.school.selfcheckout.service.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Optional wipe-and-reseed at boot, enabled with {@code --app.reset-on-startup=true}.
 *
 * Flyway has already migrated by the time ApplicationRunner beans execute, so
 * the tables exist. Other services pick the reset up through the epoch.
 */
@Component
public class StartupResetRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StartupResetRunner.class);

    private final boolean resetOnStartup;
    private final ResetService resetService;

    public StartupResetRunner(@Value("${app.reset-on-startup:false}") boolean resetOnStartup,
                              ResetService resetService) {
        this.resetOnStartup = resetOnStartup;
        this.resetService = resetService;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!resetOnStartup) {
            return;
        }
        log.info("app.reset-on-startup=true -- wiping and reseeding");
        resetService.reset();
    }
}
