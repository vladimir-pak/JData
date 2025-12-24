package com.gpb.jdata.orda.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.SchemaDTO;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.orda.repository.SchemaRepository;
import com.gpb.jdata.utils.diff.NamespaceDiffContainer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class SchemaService {

    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    @Value("${ord.api.max-connections:5}")
    private int maxConnections;

    private static final String SCHEMA_URL = "/databaseSchemas";
    
    private final SchemaRepository schemaRepository;
    private final OrdaClient ordaClient;
    private final OrdProperties ordProperties;

    private final NamespaceDiffContainer diffNamespace;

    /*
     * Новый метод. Обрабатывает только разницу snapshot
     */
    public void syncSchema() {
        String url = ordaApiUrl + SCHEMA_URL;
        ExecutorService executor = Executors.newFixedThreadPool(maxConnections);

        try {
            List<Callable<Void>> tasks = diffNamespace.getUpdated().stream()
            .<Callable<Void>>map(oid -> () -> {
                SchemaDTO body = schemaRepository.getSchemaByOid(oid);
                body.setDatabase(ordProperties.getPrefixFqn());
                ordaClient.sendPutRequest(url, body, "Создание или обновление схемы " + body.getName());
                return null;
            })
            .toList();
        
            executor.invokeAll(tasks, 10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Ошибка при синхронизации схем: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    /*
     * Новый метод. Обрабатывает только разницу snapshot
     */
    public void handleDeleted() {
        for (String fqn : diffNamespace.getDeleted()) {
            deleteSchema(fqn);
        }
    }

    public void deleteSchema(String schemaName) {
        String fqn = String.format("%s.%s", ordProperties.getPrefixFqn(), schemaName);
        try {
            String url = ordaApiUrl + SCHEMA_URL + "/name/" + fqn;
            ordaClient.sendDeleteRequest(url, "Удаление схемы " + schemaName);
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Схема не найдена: " + fqn);
            }
        } catch (Exception e) {
            log.error("Ошибка при удалении схемы{}: {}", schemaName, e.getMessage());
        }
    }

    public void handleDeletedInOrd() {
        Boolean next = true;
        String after = null;
        int page = 0;
        int limit = 100;
        String baseUrl = ordaApiUrl + SCHEMA_URL;

        Set<String> allRepSchemas = schemaRepository.findAllNspname();

        Map<String, String> params = new HashMap<>();
        params.put("database", ordProperties.getPrefixFqn());
        params.put("limit", Integer.toString(limit));
        params.put("include", "non-deleted");

        Set<String> fqnList = ConcurrentHashMap.newKeySet();

        while (next) {
            if (after != null) {
                params.put("after", after);
            }
            Map<String, Object> response = new HashMap<>();
            try {
                response.putAll(ordaClient.sendGetRequest(baseUrl, params));
            } catch (Exception e) {
                page++;
                continue;
            }
            
            page++;

            // Извлекаем данные
            List<Map<String, Object>> data = (List<Map<String, Object>>) response.get("data");
            if (data != null && !data.isEmpty()) {
                for (Map<String, Object> schema : data) {
                    fqnList.add((String) schema.get("name"));
                }
            }

            // Проверяем наличие следующей страницы
            Map<String, Object> paging = (Map<String, Object>) response.get("paging");
            if (paging != null && paging.containsKey("after")) {
                after = (String) paging.get("after");
                // Дополнительная проверка: если получено меньше данных, чем limit, значит это последняя страница
                if (data.size() < limit) {
                    next = false;
                }
            } else {
                next = false;
            }

            if (page > 100) {
                log.warn("Достигнуто максимальное количество страниц при загрузке схем: {}", page);
                break;
            }
        }

        Set<String> difference = new HashSet<>(fqnList);
        difference.removeAll(allRepSchemas);
        log.info("Найдено к удалению {} схем", difference.size());

        for (String schema : difference) {
            deleteSchema(schema);
        }
    }
}
