package com.gpb.jdata.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
@ConfigurationProperties(prefix = "greenplum")
@Data
public class DatabaseConfig {
    private String url;
    private String username;
    private String password;
    private String type;

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                getUrl(),
                getUsername(),
                getPassword());
    }
}
