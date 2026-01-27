package com.gpb.jdata.orda.client;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.orda.service.KeycloakAuthService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrdaClient {
    private final SvoiCustomLogger svoiLogger;

    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private final RestTemplate restTemplate;
    private static final String TABLE_URL = "/tables";

    private final KeycloakAuthService keycloak;

    private final ConcurrentHashMap<String, String> fqnToIdCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, String>> viewBody = new ConcurrentHashMap<>();

    public boolean isProjectEntity(String fqn) {
        String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
        try {
            HttpHeaders headers = createHeaders();
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<Map> response = restTemplate.exchange(
                url, 
                HttpMethod.GET, 
                entity, 
                Map.class
            );
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
            HttpHeaders headers = createHeaders();
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.GET, entity, Map.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                System.out.println(entityName + " существует: " + url);
                return true;
            }
        } catch (Exception e) {
            System.err.println("Ошибка при проверке существования " + entityName + ": " + url + " " + e.getMessage());
        }
        return false;
    }

    @Retryable(
        retryFor = HttpServerErrorException.InternalServerError.class,
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000)
    )
    public <T> void sendPostRequest(String url, T body, String actionDescription) {
        HttpHeaders headers = createHeaders();
        HttpEntity<T> request = new HttpEntity<>(body, headers);
        restTemplate.postForObject(url, request, Void.class);
        svoiLogger.logOrdaCall(actionDescription);
    }

    @Retryable(
        retryFor = HttpServerErrorException.InternalServerError.class,
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000)
    )
    public <T> void sendPutRequest(String url, T body, String actionDescription) {
        HttpHeaders headers = createHeaders();
        HttpEntity<T> request = new HttpEntity<>(body, headers);
        restTemplate.exchange(url, HttpMethod.PUT, request, Void.class);
        svoiLogger.logOrdaCall(actionDescription);
    }

    @Retryable(
        retryFor = HttpServerErrorException.InternalServerError.class,
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000)
    )
    public void sendDeleteRequest(String url, String actionDescription) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url)
            .queryParam("recursive", "true");
        URI uri = builder.build().toUri();
        HttpHeaders headers = createHeaders();
        HttpEntity<Void> request = new HttpEntity<>(headers);
        restTemplate.exchange(uri, HttpMethod.DELETE, request, Void.class);
        svoiLogger.logOrdaCall(actionDescription);
    }

    // Для sendPostRequest / sendPutRequest
    @Recover
    public <T> void recover(Throwable e, String url, T body, String actionDescription) {
        svoiLogger.logOrdaCall(
            "Ошибка при " + actionDescription + ": " + url + " после всех ретраев: " + e
        );
    }

    // Для sendDeleteRequest (без body)
    @Recover
    public void recover(Throwable e, String url, String actionDescription) {
        svoiLogger.logOrdaCall(
            "Ошибка при " + actionDescription + ": " + url + " после всех ретраев: " + e
        );
    }

    public Map<String, Object> sendGetRequest(String url, Map<String, String> params) throws Exception {
        try {
            MultiValueMap<String, String> multiValueParams = new LinkedMultiValueMap<>();
            if (params != null) {
                params.forEach(multiValueParams::add);
            }
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url)
                    .queryParams(multiValueParams);
            
            HttpHeaders headers = createHeaders();
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                builder.build().toUri(), HttpMethod.GET, entity, new ParameterizedTypeReference<Map<String, Object>>() {});

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            } else {
                throw new RuntimeException("HTTP ошибка: " + response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Ошибка при GET запросе: {}. {}", url, e.getMessage());
            throw e;
        }
    }

    @Retryable(
        retryFor = HttpServerErrorException.InternalServerError.class,
        maxAttempts = 2,
        backoff = @Backoff(delay = 1000)
    )
    public Optional<String> resolveTableIdByFqn(String baseUrl, String tableFqn) {
        return Optional.ofNullable(fqnToIdCache.computeIfAbsent(tableFqn, fqn -> {
            try {
                // GET /api/v1/tables/name/{fqn}
                String url = baseUrl + "/tables/name/" + fqn;
                Map<String, Object> resp = sendGetRequest(url, null);

                Object id = resp.get("id");
                if (id == null) {
                    log.warn("No 'id' in response for fqn={}", fqn);
                    return null;
                }
                return String.valueOf(id);

            } catch (Exception e) {
                log.error("Failed to resolve id for fqn={}: {}", fqn, e.getMessage());
                return null;
            }
        }));
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
