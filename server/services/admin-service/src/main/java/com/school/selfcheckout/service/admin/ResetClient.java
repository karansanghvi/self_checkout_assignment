package com.school.selfcheckout.service.admin;

import com.school.selfcheckout.client.InternalHttp;
import com.school.selfcheckout.contract.InternalDtos.ResetAck;
import com.school.selfcheckout.contract.InternalDtos.ResetCommand;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/** Sends {@code POST /internal/reset} to one service. */
@Component
public class ResetClient {

    /** A reset reseeds a database; give it far longer than a request on the hot path. */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);

    public ResetAck reset(String baseUrl, ResetCommand command) {
        RestClient client = InternalHttp.restClient(baseUrl, CONNECT_TIMEOUT, READ_TIMEOUT);
        return client.post()
                .uri("/internal/reset")
                .contentType(MediaType.APPLICATION_JSON)
                .body(command)
                .retrieve()
                .body(ResetAck.class);
    }
}
