package com.school.selfcheckout.service.catalog;

import com.school.selfcheckout.data.catalog.CatalogCache;
import com.school.selfcheckout.data.catalog.Item;
import com.school.selfcheckout.web.GlobalExceptionHandler;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class CatalogControllerTest {

    private final CatalogCache catalog = mock(CatalogCache.class);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new CatalogController(catalog))
            .setControllerAdvice(new GlobalExceptionHandler())
            .build();

    /** The load client uses list position as Zipf rank, so the cache's SKU order must survive to the wire. */
    @Test
    void itemsKeepTheCacheOrder() throws Exception {
        when(catalog.allOrderedBySku()).thenReturn(List.of(
                new Item("SKU-000001", "Item 1", new BigDecimal("0.85")),
                new Item("SKU-000002", "Item 2", new BigDecimal("1.20"))));

        mvc.perform(get("/items"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].sku").value("SKU-000001"))
                .andExpect(jsonPath("$.items[1].sku").value("SKU-000002"))
                .andExpect(jsonPath("$.items[0].price").value(0.85));
    }
}
