package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.client.OrdaClientImpl;
import com.gpb.jdata.orda.mapper.SchemaMapper;
import com.gpb.jdata.orda.repository.SchemaRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class SchemaService {
    
    private final SchemaRepository schemaRepository;
    private final OrdaClientImpl ordaClientImpl;

    public void syncSchemas() {
        try {
            List<Map<String, Object>> schemas = schemaRepository.getSchemas();
            for (Map<String, Object> schemaData : schemas) {
                Map<String, Object> body = SchemaMapper.toRequestBody(schemaData);
                ordaClientImpl.createOrUpdateSchema(body);
            }
            System.out.println("Синхронизация схем завершена успешно.");
        } catch (Exception e) {
            System.err.println("Ошибка при синхронизации схем: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void handleDeletions() {
        try {
            List<Map<String, Object>> deletedSchemas = schemaRepository.getDeletedSchemas();
            for (Map<String, Object> schemaData : deletedSchemas) {
                String fqn = "NPD_GP.adb." + schemaData.get("nspname");
                if (ordaClientImpl.checkSchemaExists(fqn)) {
                    ordaClientImpl.deleteSchema(fqn);
                    System.out.println("Схема удалена: " + fqn);
                } else {
                    System.out.println("Схема не найдена: " + fqn);
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка при обработке удалений схем: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public boolean checkSchemaExists(String fqn) {
        return ordaClientImpl.checkSchemaExists(fqn);
    }

    public void deleteSchema(String fqn) {
        try {
            ordaClientImpl.deleteSchema(fqn);
            System.out.println("Схема удалена: " + fqn);
        } catch (Exception e) {
            System.err.println("Ошибка при удалении схемы: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
