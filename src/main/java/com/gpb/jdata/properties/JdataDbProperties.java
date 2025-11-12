package com.gpb.jdata.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Component
@ConfigurationProperties(prefix = "spring.datasource")
@RequiredArgsConstructor
@Data
public class JdataDbProperties {
    private String url;

    private String username;

    private String password;
    
}
