package com.gpb.jdata.orda.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.orda.state.CleanUpState;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/cleanup")
@RequiredArgsConstructor
public class CleanUpStateController {
    private final CleanUpState cleanUpState;

    @GetMapping("/status")
    public boolean getStatus() {
        return cleanUpState.isEnabled();
    }

    @PutMapping("/enabled")
    public void setEnabled(@RequestParam boolean enabled) {
        cleanUpState.setEnabled(enabled);
    }
}
