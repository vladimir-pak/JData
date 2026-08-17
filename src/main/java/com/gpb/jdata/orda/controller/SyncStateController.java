package com.gpb.jdata.orda.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.state.SyncState;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
public class SyncStateController {
    private final SyncState syncState;

    @GetMapping("/status")
    public boolean getStatus() {
        return syncState.isEnabled();
    }

    @PutMapping("/enabled")
    public void setEnabled(@RequestParam boolean enabled) {
        syncState.setEnabled(enabled);
    }
}
