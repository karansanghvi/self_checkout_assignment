package com.school.selfcheckout.data.system;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Turns on scheduling, which {@link EpochWatcher} needs, in every service that uses this library. */
@Configuration
@EnableScheduling
public class SystemDataConfiguration {
}
