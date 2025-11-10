package com.gpb.jdata.orda.client;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.gpb.jdata.orda.OrdaClient;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrdaClientImpl implements OrdaClient {

    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private final RestTemplate restTemplate;
    private static final String DATABASE_URL = "/databases";
    private static final String SCHEMA_URL = "/databaseSchemas";
    private static final String TABLE_URL = "/tables";

    public boolean checkDatabaseExists(String databaseName) {
        String url = ordaApiUrl + DATABASE_URL + "/name/" + databaseName;
        return checkEntityExists(url, "База данных");
    }

    public void createDatabase(String databaseName) {
        String url = ordaApiUrl + DATABASE_URL;
        Map<String, Object> body = Map.of(
                "name", databaseName,
                "displayName", "ADB Database",
                "description", "Default ADB database"
        );
        sendPostRequest(url, body, "Создание базы данных");
    }

    public boolean checkSchemaExists(String fqn) {
        String url = ordaApiUrl + SCHEMA_URL + "/name/" + fqn;
        return checkEntityExists(url, "Схема");
    }

    public void createOrUpdateSchema(Map<String, Object> schemaData) {
        String url = ordaApiUrl + SCHEMA_URL;
        sendPutRequest(url, schemaData, "Создание или обновление схемы");
    }

    public void deleteSchema(String fqn) {
        String url = ordaApiUrl + SCHEMA_URL + "/name/" + fqn;
        sendDeleteRequest(url, "Удаление схемы");
    }

    public void createOrUpdateTable(Map<String, Object> tableData) {
        String url = ordaApiUrl + TABLE_URL;
        sendPutRequest(url, tableData, "Создание или обновление таблицы");
    }

    public boolean isProjectEntity(String fqn) {
        String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return Boolean.TRUE.equals(response.getBody().get("isProjectEntity"));
            }
        } catch (Exception e) {
            System.err.println("Ошибка при проверке isProjectEntity для таблицы: " + fqn + ". " + e.getMessage());
        }
        return false;
    }

    public void deleteTable(String fqn) {
        String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
        sendDeleteRequest(url, "Удаление таблицы");
    }

    private boolean checkEntityExists(String url, String entityName) {
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                System.out.println(entityName + " существует: " + url);
                return true;
            }
        } catch (Exception e) {
            System.err.println("Ошибка при проверке существования " + entityName + ": " + url + ". " + e.getMessage());
        }
        return false;
    }

    public boolean checkTableExists(String fqn) {
        String url = ordaApiUrl + "/v1/tables/name/" + fqn;
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            return response.getStatusCode().is2xxSuccessful() && response.getBody() != null;
        } catch (Exception e) {
            System.err.println("Ошибка при проверке существования таблицы: " + fqn + ". " + e.getMessage());
            return false;
        }
    }

    private void sendPostRequest(String url, Map<String, Object> body, String actionDescription) {
        try {
            HttpHeaders headers = createHeaders();
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);
            restTemplate.postForObject(url, request, Void.class);
            System.out.println(actionDescription + " выполнено успешно: " + url);
        } catch (Exception e) {
            System.err.println("Ошибка при " + actionDescription + ": " + url + ". " + e.getMessage());
            }
    }

    private void sendPutRequest(String url, Map<String, Object> body, String actionDescription) {
        try {
            HttpHeaders headers = createHeaders();
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);
            restTemplate.exchange(url, HttpMethod.PUT, request, Void.class);
            System.out.println(actionDescription + " выполнено успешно: " + url);
        } catch (Exception e) {
            System.err.println("Ошибка при " + actionDescription + ": " + url + ". " + e.getMessage());
        }
    }

    private void sendDeleteRequest(String url, String actionDescription) {
        try {
            restTemplate.delete(url);
            System.out.println(actionDescription + " выполнено успешно: " + url);
        } catch (Exception e) {
            System.err.println("Ошибка при " + actionDescription + ": " + url + ". " + e.getMessage());
        }
    }
    
    private HttpHeaders createHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        return headers;
    }
}
