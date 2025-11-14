package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.concurrent.Callable;
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
        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Callable<Void>> tasks = diffNamespace.getUpdated().stream()
            .<Callable<Void>>map(oid -> () -> {
                SchemaDTO body = schemaRepository.getSchemaByOid(oid);
                body.setDatabase(String.format("%s.%s", ordProperties.getPrefixFqn(), body.getName()));
                ordaClient.sendPutRequest(url, body, "Создание или обновление схемы");
                return null;
            })
            .toList();
        try {
            executor.invokeAll(tasks, 1, TimeUnit.MINUTES);
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
            ordaClient.sendDeleteRequest(url, "Удаление схемы");
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Схема не найдена: " + fqn);
            }
        } catch (Exception e) {
            log.error("Ошибка при удалении схемы: " + e.getMessage());
        }
    }
}
