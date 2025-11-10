package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.client.OrdaClientImpl;
import com.gpb.jdata.orda.repository.SchemaRepository;
import com.gpb.jdata.orda.repository.TableRepository;
import com.gpb.jdata.orda.util.OrdaUtils;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DeletionTrackingService {
    private final SchemaRepository schemaRepository;
    private final TableRepository tableRepository;
    private final OrdaClientImpl ordaClientImpl;

    public void handleSchemaDeletions() {
        List<Map<String, Object>> deletedSchemas = schemaRepository.getDeletedSchemas();
        for (Map<String, Object> s : deletedSchemas) {
            String fqn = OrdaUtils.fqnSchema((String) s.get("nspname"));
            if (ordaClientImpl.checkSchemaExists(fqn)) {
                ordaClientImpl.deleteSchema(fqn);
            }
        }
    }

    public void handleTableDeletions() {
        List<Map<String, Object>> deletedTables = tableRepository.getDeletedTables();
        for (Map<String, Object> t : deletedTables) {
            String fqn = OrdaUtils.fqnTable((String) t.get("schemaname"), (String) t.get("tablename"));
            if (ordaClientImpl.checkTableExists(fqn) && !ordaClientImpl.isProjectEntity(fqn)) {
                ordaClientImpl.deleteTable(fqn);
            }
        }
    }
}
