package com.gpb.jdata.orda.properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Component
@ConfigurationProperties(prefix = "ord.greenplum")
@RequiredArgsConstructor
@Data
public class OrdProperties {
    private String serviceName;
    private String dbName;

    @Value("${ord.api.baseUrl}")
    private String baseUrl;

    @Value("${keycloak.username}")
    private String username;

    public String getPrefixFqn () {
        return String.format("%s.%s", serviceName, dbName);
    }
}
