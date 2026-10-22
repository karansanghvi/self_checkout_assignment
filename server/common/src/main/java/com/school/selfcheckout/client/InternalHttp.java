package com.school.selfcheckout.client;

import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Builds the RestClient every service uses to call another one.
 *
 * Pinned to HTTP/1.1: over plain http the JDK client would otherwise try an
 * h2c upgrade on each new connection, which Tomcat answers by staying on 1.1
 * anyway. The JDK client pools and reuses connections itself.
 *
 * The timeouts are the whole failure-detection story between services, so
 * each caller chooses its own rather than inheriting a default.
 */
public final class InternalHttp {

    private InternalHttp() {
    }

    public static RestClient restClient(String baseUrl, Duration connectTimeout, Duration readTimeout) {
        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(connectTimeout)
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(readTimeout);
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(factory)
                .build();
    }
}
