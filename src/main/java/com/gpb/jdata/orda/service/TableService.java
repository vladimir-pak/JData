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
import com.gpb.jdata.orda.dto.TableDTO;
import com.gpb.jdata.orda.dto.ViewDTO;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.orda.repository.TableRepository;
import com.gpb.jdata.utils.diff.ClassDiffContainer;
import com.gpb.jdata.utils.diff.ViewDiffContainer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class TableService {
    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    @Value("${ord.api.max-connections:5}")
    private int maxConnections;

    private static final String TABLE_URL = "/tables";
    
    private final TableRepository tableRepository;
    private final OrdaClient ordaClient;

    private final OrdProperties ordProperties;

    private final ClassDiffContainer diffContainer;
    private final ViewDiffContainer viewDiffContainer;

    public void syncTables() {
        ExecutorService executor = Executors.newFixedThreadPool(maxConnections);

        try {
            viewDiffContainer.clear();
            List<Callable<Void>> tasks = diffContainer.getUpdated().stream()
                .<Callable<Void>>map(oid -> () -> {
                    try {
                        putTable(oid);
                    } catch (Exception e) {
                        log.error("Ошибка при обработке таблицы oid={}: {}", oid, e.getMessage(), e);
                    }
                    return null;
                })
                .toList();
            executor.invokeAll(tasks, 60, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Ошибка при синхронизации таблицы: {}", e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    public void putTable(Long oid) {
        TableDTO table = tableRepository.findByOid(oid);
        
        if (table == null) {
            log.warn("Таблица oid={} пустая (null)", oid);
            return;
        }

        String fqn = String.format("%s.%s", 
                table.getDatabaseSchema(), table.getName());

        try {
            // Если таблица - проектная сушность, то не меняем метаданные по ней
            if (ordaClient.isProjectEntity(fqn)) {
                return;
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
                log.warn("Ошибка при проверке таблицы в ОРДе {}: {}", fqn, e.getMessage());
                return;
            }
            // Если таблица не найдена (404) ответ, то делаем PUT запрос
        } catch (Exception e) {
            log.warn("Ошибка при проверке таблицы в ОРДе {}: {}", fqn, e.getMessage());
            return;
        }

        if (table.getTableType() == "View") {
            ViewDTO view = new ViewDTO();
            view.setViewName(table.getName());
            view.setSchemaName(lastTokenAfterDot(table.getDatabaseSchema())); // передаем схему без service.db
            view.setViewDefinition(table.getViewDefinition());
            viewDiffContainer.addUpdated(view);

            // deprecated - баг устранен
            // String deleteUrl = ordaApiUrl + TABLE_URL + "/name/" + fqn;
            // ordaClient.sendDeleteRequest(deleteUrl, "Удаление таблицы view " + fqn);
        }

        String url = ordaApiUrl + TABLE_URL;
        
        ordaClient.sendPutRequest(url, table, 
                String.format("Создание или обновление таблицы %s.%s", 
                        table.getDatabaseSchema(), table.getName()));
    }

    public void handleDeleted() {
        for (String tableName : diffContainer.getDeleted()) {
            // tableName - schema.tablename
            deleteTable(tableName);
        }
    }

    public void deleteTable(String tableName) {
        String fqn = String.format("%s.%s", ordProperties.getPrefixFqn(), tableName);
        try {
            String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
            if (!ordaClient.isProjectEntity(fqn)) {
                ordaClient.sendDeleteRequest(url, "Удаление таблицы " + fqn);
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Таблица не найдена: {}", fqn);
            }
        } catch (Exception e) {
            log.error("Ошибка при удалении таблицы {}: {}", fqn, e.getMessage());
        }
    }

    public void deleteTableByFqn(String fqn) {
        try {
            String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
            if (!ordaClient.isProjectEntity(fqn)) {
                ordaClient.sendDeleteRequest(url, "Удаление таблицы " + fqn);
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Таблица не найдена: {}", fqn);
            }
        } catch (Exception e) {
            log.error("Ошибка при удалении таблицы {}: {}", fqn, e.getMessage());
        }
    }

    private static String lastTokenAfterDot(String s) {
        if (s == null) return null;
        int idx = s.lastIndexOf('.');
        return (idx >= 0 && idx < s.length() - 1) ? s.substring(idx + 1) : s;
    }
}
