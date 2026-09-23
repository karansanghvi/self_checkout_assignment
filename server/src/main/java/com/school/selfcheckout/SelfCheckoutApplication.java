package com.school.selfcheckout;

import com.school.selfcheckout.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class SelfCheckoutApplication {

    public static void main(String[] args) {
        SpringApplication.run(SelfCheckoutApplication.class, args);
    }
}
