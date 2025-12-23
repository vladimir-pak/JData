package com.gpb.jdata.orda.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.repository.SchemaRepository;
import com.gpb.jdata.orda.repository.TableRepository;
import com.gpb.jdata.utils.diff.ClassDiffContainer;
import com.gpb.jdata.utils.diff.NamespaceDiffContainer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class MetadataService {
    private final SchemaService schemaService;
    private final TableService tableService;
    private final DatabaseService databaseService;
    private final ClassDiffContainer classDiffContainer;
    private final NamespaceDiffContainer namespaceDiffContainer;

    private final SchemaRepository schemaRepository;
    private final TableRepository tableRepository;
    
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataService.class);

    public void initialMetadata() {
        try {
            classDiffContainer.clear();
            namespaceDiffContainer.clear();

            List<Long> schemas = schemaRepository.findAll();
            namespaceDiffContainer.addAllUpdated(schemas);

            List<Long> tables = tableRepository.findAll();
            classDiffContainer.addAllUpdated(tables);

            syncMetadata();
        } catch (Exception e) {
            logger.error("Ошибка при инициализации метаданных: {}", e.getMessage(), e);
        }
    }

    public void syncMetadata() {
        try {
            databaseService.checkAndCreateDatabase();
            syncSchemas();
            syncTables();
            handleDeletions();
            logger.info("Синхронизация метаданных завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации метаданных: {}", e.getMessage(), e);
        }
    }

    public void syncSchemas() {
        try {
            schemaService.syncSchema();
            logger.info("Синхронизация схем завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации схем: {}", e.getMessage(), e);
        }
    }

    public void syncTables() {
        try {
            tableService.syncTables();
            logger.info("Синхронизация таблиц завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации таблиц: {}", e.getMessage(), e);
        }
    }
    
    public void handleDeletions() {
        try {
            schemaService.handleDeleted();
            tableService.handleDeleted();
            logger.info("Обработка удалений завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при обработке удалений: {}", e.getMessage(), e);
        }
    }

    public void handleDeletionsInOrd() {
        try {
            schemaService.handleDeletedInOrd();
            tableService.handleDeletedInOrd();
            logger.info("Обработка удалений завершена успешно.");
        } catch (Exception e) {
            logger.error("Ошибка при обработке удалений: {}", e.getMessage(), e);
        }
    }
}
