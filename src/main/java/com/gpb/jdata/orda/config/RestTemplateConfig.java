package com.gpb.jdata.orda.config;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.TimeValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {

        // Новый способ указания проверки простаивающих соединений
        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setValidateAfterInactivity(TimeValue.ofSeconds(1))
                .build();

        PoolingHttpClientConnectionManager cm =
                new PoolingHttpClientConnectionManager();
        cm.setDefaultConnectionConfig(connectionConfig);
        cm.setMaxTotal(100);
        cm.setDefaultMaxPerRoute(20);

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofSeconds(30))
                .build();

        HttpComponentsClientHttpRequestFactory requestFactory =
                new HttpComponentsClientHttpRequestFactory(httpClient);

        requestFactory.setConnectTimeout(5000);
        requestFactory.setReadTimeout(15000);

        return new RestTemplate(requestFactory);
    }
}
