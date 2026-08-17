package com.gpb.jdata.orda.state;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import com.gpb.jdata.properties.SyncProperties;

@Component
public class SyncState {

    private final AtomicBoolean enabled;

    public SyncState(SyncProperties syncProperties) {
        this.enabled = new AtomicBoolean(syncProperties.isEnabled());
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