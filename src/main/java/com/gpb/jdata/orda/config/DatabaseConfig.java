package com.gpb.jdata.orda.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.RequiredArgsConstructor;

@Configuration
@ConfigurationProperties(prefix = "database")
@RequiredArgsConstructor
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    public String getDatabaseName() {
        return url.substring(url.lastIndexOf("/") + 1);
    }

    public static final String ADB_DATABASE_NAME = "adb";
    
    public boolean isADBDatabase() {
        return ADB_DATABASE_NAME.equals(getDatabaseName());
    }
}
