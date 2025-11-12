package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class TableService {
    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private static final String TABLE_URL = "/tables";
    
    private final TableRepository tableRepository;
    private final PartitionRepository partitionRepository;
    private final OrdaClient ordaClient;

    private final OrdProperties ordProperties;

    public void syncTables(String schemaName) {
        List<Map<String, Object>> rows = tableRepository.getTables(schemaName);
        sendGrouped(rows);
    }

    public void syncAllTables() {
        List<Map<String, Object>> rows = tableRepository.getAllTables();
        sendGrouped(rows);
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

    public void handleDeletions() {
        List<Map<String, Object>> deletedTables = tableRepository.getDeletedTables();
        for (Map<String, Object> t : deletedTables) {
            String fqn = String.format("%s.%s.%s", 
                    ordProperties.getPrefixFqn(), (String)t.get("schemaname"), (String)t.get("tablename"));
            if (!ordaClient.isProjectEntity(fqn)) {
                deleteTable(fqn);
            }
        }
    }

    public void deleteTable(String fqn) {
        try {
            String url = ordaApiUrl + TABLE_URL + "/name/" + fqn;
            ordaClient.sendDeleteRequest(url, "Удаление таблицы");
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Таблица не найдена: " + fqn);
            }
        } catch (Exception e) {
            System.err.println("Ошибка при удалении таблицы: " + e.getMessage());
            e.printStackTrace();
        }
    }

    
}
