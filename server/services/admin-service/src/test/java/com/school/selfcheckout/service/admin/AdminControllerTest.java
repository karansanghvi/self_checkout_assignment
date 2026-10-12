package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.web.GlobalExceptionHandler;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class AdminControllerTest {

    private final ResetService resetService = mock(ResetService.class);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new AdminController(resetService))
            .setControllerAdvice(new GlobalExceptionHandler())
            .build();

    @Test
    void resetPassesAbsentSizesThroughAsNull() throws Exception {
        when(resetService.reset(any(), any())).thenReturn(Map.of("status", "ok"));

        mvc.perform(post("/admin/reset")).andExpect(status().isOk());

        // Previously the controller substituted hardcoded 2000/10000 here.
        verify(resetService).reset(null, null);
    }
}
