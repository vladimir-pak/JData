package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.SchemaDTO;
import com.gpb.jdata.orda.mapper.SchemaMapper;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.orda.repository.SchemaRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class SchemaService {

    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    private static final String SCHEMA_URL = "/databaseSchemas";
    
    private final SchemaRepository schemaRepository;
    private final OrdaClient ordaClient;
    private final OrdProperties ordProperties;

    public void syncSchemas() {
        try {
            List<Map<String, Object>> schemas = schemaRepository.getSchemas();
            for (Map<String, Object> schemaData : schemas) {
                SchemaDTO body = SchemaMapper.toRequestBody(schemaData, ordProperties.getPrefixFqn());
                String url = ordaApiUrl + SCHEMA_URL;
                ordaClient.sendPutRequest(url, body, "Создание или обновление схемы");
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
            for (Map<String, Object> data : deletedSchemas) {
                String fqn = String.format(
                        "%s.%s", 
                        ordProperties.getPrefixFqn(), data.get("nspname"));
                deleteSchema(fqn);
            }
        } catch (Exception e) {
            System.err.println("Ошибка при обработке удалений схем: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void deleteSchema(String fqn) {
        try {
            String url = ordaApiUrl + SCHEMA_URL + "/name/" + fqn;
            ordaClient.sendDeleteRequest(url, "Удаление схемы");
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                System.out.println("Схема не найдена: " + fqn);
            }
        } catch (Exception e) {
            System.err.println("Ошибка при удалении схемы: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
