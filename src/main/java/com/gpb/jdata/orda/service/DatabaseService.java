package com.gpb.jdata.orda.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.DatabaseDTO;
import com.gpb.jdata.orda.properties.OrdProperties;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DatabaseService {

    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private static final String DATABASE_URL = "/databases";

    private final OrdProperties ordProperties;
    private final OrdaClient ordaClient;

    private boolean checkDatabaseExists() {
        String fqn = ordProperties.getPrefixFqn();
        String url = ordaApiUrl + "/databases/name/" + fqn;
        return ordaClient.checkEntityExists(url, "База данных");
    }

    private void createDatabase() {
        String url = ordaApiUrl + DATABASE_URL;
        DatabaseDTO body = DatabaseDTO.builder()
                .name(ordProperties.getDbName())
                .displayName(ordProperties.getDbName())
                .service(ordProperties.getServiceName())
                .build();
        ordaClient.sendPostRequest(url, body, "Создание базы данных");
    }

    /*
     * Проверка наличия БД в ОРДе.
     * Создание в случае отсутствия.
     */
    public void checkAndCreateDatabase() {
        if (!checkDatabaseExists()) {
            createDatabase();
        }
    }
    
    /*
     * Получение БД
     */
    public String getCurrentDatabaseName() {
        return ordProperties.getDbName();
    }
}
