package com.gpb.jdata.orda.state;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.properties.OrdCleanupProperties;

@Component
public class CleanUpState {

    private final AtomicBoolean enabled;

    public CleanUpState(OrdCleanupProperties ordCleanupProperties) {
        this.enabled = new AtomicBoolean(ordCleanupProperties.isEnabled());
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public boolean enable() {
        return enabled.compareAndSet(false, true);
    }

    public boolean disable() {
        return enabled.compareAndSet(true, false);
    }
}