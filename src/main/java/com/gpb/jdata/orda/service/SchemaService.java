package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.BadSqlGrammarException;
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
    // public void syncSchema() {
    //     String url = ordaApiUrl + SCHEMA_URL;
    //     ExecutorService executor = Executors.newFixedThreadPool(5);

    //     try {
    //         List<Callable<Void>> tasks = diffNamespace.getUpdated().stream()
    //         .<Callable<Void>>map(oid -> () -> {
    //             SchemaDTO body = schemaRepository.getSchemaByOid(oid);
    //             body.setDatabase(String.format("%s.%s", ordProperties.getPrefixFqn(), body.getName()));
    //             ordaClient.sendPutRequest(url, body, "Создание или обновление схемы");
    //             return null;
    //         })
    //         .toList();
        
    //         executor.invokeAll(tasks, 1, TimeUnit.MINUTES);
    //     } catch (InterruptedException e) {
    //         Thread.currentThread().interrupt();
    //         log.error("Ошибка при синхронизации схем: " + e.getMessage());
    //     } catch (BadSqlGrammarException e) {
    //         throw e;
    //     } finally {
    //         executor.shutdown();
    //     }
    // }

    public void syncSchema() {
        String url = ordaApiUrl + SCHEMA_URL;
        int poolSize = maxConnections;
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        try {
            // Формируем список задач
            List<Callable<Void>> tasks = diffNamespace.getUpdated().stream()
                    .<Callable<Void>>map(oid -> () -> {
                        SchemaDTO body = schemaRepository.getSchemaByOid(oid);
                        if (body == null) {
                            log.warn("Схема не найдена для oid={}", oid);
                            return null;
                        }
                        body.setDatabase(ordProperties.getPrefixFqn());
                        ordaClient.sendPutRequest(url, body, "Создание или обновление схемы");
                        return null;
                    })
                    .toList();

            // Запуск всех задач с таймаутом
            List<Future<Void>> futures = executor.invokeAll(tasks, 1, TimeUnit.MINUTES);

            // Проверяем результаты и ловим ошибки
            for (Future<Void> future : futures) {
                try {
                    future.get(); // выбросит ExecutionException если задача упала
                } catch (ExecutionException e) {
                    // Прерываем все остальные задачи
                    for (Future<Void> f : futures) {
                        f.cancel(true);
                    }
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    } else if (cause instanceof Exception) {
                        throw new RuntimeException(cause);
                    } else {
                        throw new RuntimeException("Неизвестная ошибка при синхронизации схем", cause);
                    }
                } catch (CancellationException e) {
                    log.warn("Задача была отменена из-за ошибки в другой задаче");
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Синхронизация схем прервана: {}", e.getMessage(), e);
        } catch (BadSqlGrammarException e) {
            throw e;
        } finally {
            executor.shutdownNow();
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
