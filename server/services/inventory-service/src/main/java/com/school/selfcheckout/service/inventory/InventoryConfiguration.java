package com.school.selfcheckout.service.inventory;

import com.school.selfcheckout.client.CatalogReplica;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InventoryConfiguration {

    /** Used only at reset, to learn which SKUs to stock and what they are called. */
    @Bean
    public CatalogReplica catalogReplica(@Value("${app.catalog-url}") String catalogUrl) {
        return new CatalogReplica(catalogUrl);
    }
}
