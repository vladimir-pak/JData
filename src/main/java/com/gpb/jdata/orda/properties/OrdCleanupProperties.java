package com.gpb.jdata.orda.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Component
@ConfigurationProperties(prefix = "ord.greenplum.cleanup")
@RequiredArgsConstructor
@Data
public class OrdCleanupProperties {
    private boolean enabled;
    private boolean validate;
    private String schedule;
    private int threshold;
}
