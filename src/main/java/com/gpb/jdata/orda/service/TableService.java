package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.client.OrdaClientImpl;
import com.gpb.jdata.orda.mapper.TableMapper;
import com.gpb.jdata.orda.repository.PartitionRepository;
import com.gpb.jdata.orda.repository.TableRepository;
import com.gpb.jdata.orda.util.OrdaUtils;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class TableService {
    private final TableRepository tableRepository;
    private final PartitionRepository partitionRepository;
    private final OrdaClientImpl ordaClientImpl;

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
            Map<String, Object> body = TableMapper.toRequestBody(tableRows, parts, tableRepository);
            ordaClientImpl.createOrUpdateTable(body);
        }
    }

    public void handleDeletions() {
        List<Map<String, Object>> deletedTables = tableRepository.getDeletedTables();
        for (Map<String, Object> t : deletedTables) {
            String fqn = OrdaUtils.fqnTable((String)t.get("schemaname"), (String)t.get("tablename"));
            if (ordaClientImpl.checkTableExists(fqn) && !ordaClientImpl.isProjectEntity(fqn)) {
                ordaClientImpl.deleteTable(fqn);
            }
        }
    }

    public void deleteTable(String fqn) {
        if (!ordaClientImpl.isProjectEntity(fqn)) {
            ordaClientImpl.deleteTable(fqn);
        } else {
            throw new IllegalArgumentException("Нельзя удалить таблицу с isProjectEntity = true");
        }
    }
}
