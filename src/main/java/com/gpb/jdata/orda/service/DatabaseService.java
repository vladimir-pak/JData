package com.gpb.jdata.orda.service;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.client.OrdaClientImpl;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DatabaseService {

    private final OrdaClientImpl ordaClientImpl;

    public void checkAndCreateDatabase(String databaseName) {
        if (!ordaClientImpl.checkDatabaseExists(databaseName)) {
            ordaClientImpl.createDatabase(databaseName);
        }
    }
    
    public String getCurrentDatabaseName() {
        return "adb";
    }
}
