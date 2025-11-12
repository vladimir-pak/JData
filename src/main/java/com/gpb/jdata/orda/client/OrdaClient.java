package com.gpb.jdata.orda.client;

import java.util.Arrays;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.gpb.jdata.orda.service.KeycloakAuthService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrdaClient {

    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private final RestTemplate restTemplate;
    private static final String TABLE_URL = "/tables";

    private final KeycloakAuthService keycloak;

    public boolean isProjectEntity(String fqn) {
        String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return Boolean.TRUE.equals(response.getBody().get("isProjectEntity"));
            }
        } catch (Exception e) {
            System.err.println("Ошибка при проверке isProjectEntity для таблицы: " + fqn + ". " + e.getMessage());
            return true;
        }
        return false;
    }

    public boolean checkEntityExists(String url, String entityName) {
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

    public <T> void sendPostRequest(String url, T body, String actionDescription) {
        try {
            HttpHeaders headers = createHeaders();
            HttpEntity<T> request = new HttpEntity<>(body, headers);
            restTemplate.postForObject(url, request, Void.class);
            System.out.println(actionDescription + " выполнено успешно: " + url);
        } catch (Exception e) {
            System.err.println("Ошибка при " + actionDescription + ": " + url + ". " + e.getMessage());
            }
    }

    public <T> void sendPutRequest(String url, T body, String actionDescription) {
        try {
            HttpHeaders headers = createHeaders();
            HttpEntity<T> request = new HttpEntity<>(body, headers);
            restTemplate.exchange(url, HttpMethod.PUT, request, Void.class);
            System.out.println(actionDescription + " выполнено успешно: " + url);
        } catch (Exception e) {
            System.err.println("Ошибка при " + actionDescription + ": " + url + ". " + e.getMessage());
        }
    }

    public void sendDeleteRequest(String url, String actionDescription) {
        try {
            restTemplate.delete(url);
            System.out.println(actionDescription + " выполнено успешно: " + url);
        } catch (HttpClientErrorException e) {
            System.err.println("HTTP ошибка " + e.getStatusCode() 
                    + " при " + actionDescription + ": " + url);
            throw e;
        } catch (Exception e) {
            System.err.println("Ошибка при " + actionDescription + ": " + url + ". " + e.getMessage());
        }
    }
    
    private HttpHeaders createHeaders() {
        String token = keycloak.getValidAccessToken();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        headers.setBearerAuth(token);
        return headers;
    }
}
