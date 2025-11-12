package com.gpb.jdata.orda.service;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class MetadataService {
    private static final String DEFAULT_DATABASE_NAME = "adb";
    
    private final DatabaseService databaseService;
    private final SchemaService schemaService;
    private final TableService tableService;
    
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataService.class);

    public void syncMetadata() {
        try {
            syncDatabase();
            syncSchemas();
            syncTables();
            handleDeletions();
            logger.info("Синхронизация метаданных завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации метаданных: {}", e.getMessage(), e);
        }
    }

    public void syncDatabase() {
        try {
            databaseService.checkAndCreateDatabase(DEFAULT_DATABASE_NAME);
            logger.info("Проверка и создание базы данных выполнены успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при проверке и создании базы данных: {}", e.getMessage(), e);
        }
    }

    public void syncSchemas() {
        try {
            schemaService.syncSchemas();
            logger.info("Синхронизация схем завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации схем: {}", e.getMessage(), e);
        }
    }

    public void syncTables() {
        try {
            tableService.syncAllTables();
            logger.info("Синхронизация таблиц завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации таблиц: {}", e.getMessage(), e);
        }
    }
    
    public void handleDeletions() {
        try {
            // deletionTrackingService.handleSchemaDeletions();
            schemaService.handleDeletions();
            tableService.handleDeletions();
            logger.info("Обработка удалений завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при обработке удалений: {}", e.getMessage(), e);
        }
    }
}
