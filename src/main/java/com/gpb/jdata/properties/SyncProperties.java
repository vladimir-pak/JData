package com.gpb.jdata.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "sync")
public class SyncProperties {
    private boolean enabled = true;
    private long interval = 60000; // по умолчанию 60 секунд
}
