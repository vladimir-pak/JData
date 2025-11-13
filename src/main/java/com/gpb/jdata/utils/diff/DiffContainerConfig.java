package com.gpb.jdata.utils.diff;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DiffContainerConfig {

    @Bean
    public DiffContainer pgNamespaceDiffContainer() {
        return new DiffContainer();
    }

    @Bean
    public DiffContainer pgClassDiffContainer() {
        return new DiffContainer();
    }
}
