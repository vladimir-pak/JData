package com.gpb.jdata.properties;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
@Configuration
@ConfigurationProperties(prefix = "spring.application")
@RequiredArgsConstructor
@Data
public class SysProperties {
    private String name;
    private String version;
    private String dntdom;
}
