package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.TableDTO;
import com.gpb.jdata.orda.mapper.TableMapper;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.orda.repository.PartitionRepository;
import com.gpb.jdata.orda.repository.TableRepository;
import com.gpb.jdata.utils.diff.DiffContainer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class TableService {
    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private static final String TABLE_URL = "/tables";
    
    private final TableRepository tableRepository;
    private final PartitionRepository partitionRepository;
    private final OrdaClient ordaClient;

    private final OrdProperties ordProperties;

    @Qualifier("pgClassDiffContainer")
    private final DiffContainer diffContainer;

    public void syncTables() {
        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Callable<Void>> tasks = diffContainer.getUpdated().stream()
            .<Callable<Void>>map(oid -> () -> {
                try {
                    List<Map<String, Object>> rows = tableRepository.getTableByOid(oid);
                    if (rows != null && !rows.isEmpty()) {
                        sendGrouped(rows);
                    }
                } catch (Exception e) {
                    log.error("Ошибка при обработке таблицы oid={}: {}", oid, e.getMessage(), e);
                }
                return null;
            })
            .toList();
        try{
            executor.invokeAll(tasks, 1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Ошибка при синхронизации таблицы: {}", e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    private void sendGrouped(List<Map<String, Object>> rows) {
        Map<String, List<Map<String, Object>>> byTable = rows.stream().collect(
                Collectors.groupingBy(r -> ((String) r.get("dbschema")) + "|" + ((String) r.get("tablename")))
        );
        for (Map.Entry<String, List<Map<String, Object>>> e : byTable.entrySet()) {
            List<Map<String, Object>> tableRows = e.getValue();
            String schema = (String) tableRows.get(0).get("dbschema");
            String table  = (String) tableRows.get(0).get("tablename");
            List<Map<String, Object>> parts = partitionRepository.getPartitions(schema, table);
            TableDTO body = TableMapper.toRequestBody(
                        tableRows, parts, tableRepository, ordProperties.getPrefixFqn());
            String url = ordaApiUrl + TABLE_URL;
            ordaClient.sendPutRequest(url, body, "Создание или обновление таблицы");
        }
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
            ordaClient.sendDeleteRequest(url, "Удаление таблицы");
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Таблица не найдена: {}", fqn);
            }
        } catch (Exception e) {
            log.error("Ошибка при удалении таблицы: {}", e.getMessage());
        }
    }
}
