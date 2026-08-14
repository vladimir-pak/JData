package com.gpb.jdata.orda.service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.properties.OrdCleanupProperties;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.orda.repository.OpenMetadataRepository;
import com.gpb.jdata.orda.repository.SchemaRepository;
import com.gpb.jdata.orda.repository.TableRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class CleanUpService {

    private final AtomicBoolean running = new AtomicBoolean(false);

    private static final long DELETE_TIMEOUT_MINUTES = 60;

    private final OrdProperties ordProperties;
    private final OrdCleanupProperties ordCleanupProperties;

    private final TableRepository tableRepository;
    private final SchemaRepository schemaRepository;
    private final OpenMetadataRepository ordRepository;
    private final TableService tableService;
    private final SchemaService schemaService;

    public boolean start(boolean validate) {
        if (!running.compareAndSet(false, true)) {
            log.warn("Очистка OMD уже выполняется. Новый запуск пропущен");
            return false;
        }

        try {
            log.info("Запуск очистки OMD");

            cleanUpTables(validate);
            cleanUpSchemas(validate);

            log.info("Очистка OMD завершена");
            return true;
        } finally {
            running.set(false);
        }
    }

    private void cleanUpTables(boolean validate) {
        Set<String> tablesToDelete = getTablesToDelete(validate);

        log.info(
                "Найдено таблиц для удаления: {}",
                tablesToDelete.size()
        );

        if (tablesToDelete.isEmpty()) {
            log.info("Таблицы для удаления отсутствуют");
            return;
        }

        deleteEntities(
                tablesToDelete,
                "таблиц",
                tableService::deleteTableByFqn
        );
    }

    private void cleanUpSchemas(boolean validate) {
        Set<String> schemasToDelete = getSchemasToDelete(validate);

        log.info(
                "Найдено схем для удаления: {}",
                schemasToDelete.size()
        );

        if (schemasToDelete.isEmpty()) {
            log.info("Схемы для удаления отсутствуют");
            return;
        }

        deleteEntities(
                schemasToDelete,
                "схем",
                schemaService::deleteSchema
        );
    }

    private Set<String> getTablesToDelete(boolean validate) {
        return findEntitiesToDelete(
                ordRepository.findAllTables(),
                tableRepository.findAllNames(),
                "таблиц",
                validate
        );
    }

    private Set<String> getSchemasToDelete(boolean validate) {
        return findEntitiesToDelete(
                ordRepository.findAllSchemas(),
                schemaRepository.findAllNspname(),
                "схем",
                validate
        );
    }

    private Set<String> findEntitiesToDelete(
            Set<String> ordEntities,
            Set<String> dbEntities,
            String entityType,
            boolean validate
    ) {
        log.info(
                "{} для cleanup: OMD={}, DB={}",
                entityType,
                ordEntities.size(),
                dbEntities.size()
        );

        if (dbEntities.isEmpty() && !ordEntities.isEmpty()) {
            log.warn(
                    "Получен пустой список {} из БД при наличии {} в OMD. " +
                    "Очистка отменена.",
                    entityType,
                    ordEntities.size()
            );

            return Collections.emptySet();
        }

        Set<String> dbFqns = dbEntities.stream()
                .map(this::toFqn)
                .collect(Collectors.toSet());

        Set<String> toDelete = ordEntities.stream()
                .filter(fqn -> !dbFqns.contains(fqn))
                .collect(Collectors.toSet());

        if (validate
                && !validateThreshold(
                        toDelete.size(),
                        ordEntities.size(),
                        entityType
                )) {

            log.warn(
                    "Аномальное количество удаляемых {}. " +
                    "На удаление={}. Всего={}. Threshold={}%",
                    entityType,
                    toDelete.size(),
                    ordEntities.size(),
                    ordCleanupProperties.getThreshold()
            );

            return Collections.emptySet();
        }

        return toDelete;
    }

    private String toFqn(String entity) {
        return String.join(
                ".",
                ordProperties.getServiceName(),
                ordProperties.getDbName(),
                entity
        );
    }

    private boolean validateThreshold(
            int deletedCount,
            int totalCount,
            String entityType
    ) {
        if (totalCount == 0) {
            return deletedCount == 0;
        }

        double percentage =
                deletedCount * 100.0 / totalCount;

        log.info(
                "Проверка threshold для {}: удаляется {} из {}, " +
                "процент={}, threshold={}%",
                entityType,
                deletedCount,
                totalCount,
                percentage,
                ordCleanupProperties.getThreshold()
        );

        return percentage <= ordCleanupProperties.getThreshold();
    }

    private void deleteEntities(
            Set<String> entities,
            String entityType,
            Consumer<String> deleteAction
    ) {
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        ordProperties.getMaxConnections()
                );

        try {
            List<Callable<Void>> tasks = entities.stream()
                    .<Callable<Void>>map(entity -> () -> {
                        try {
                            deleteAction.accept(entity);
                        } catch (Exception e) {
                            log.error(
                                    "Ошибка при удалении {} {}: {}",
                                    entityType,
                                    entity,
                                    e.getMessage(),
                                    e
                            );
                        }

                        return null;
                    })
                    .toList();

            List<Future<Void>> futures =
                    executor.invokeAll(
                            tasks,
                            DELETE_TIMEOUT_MINUTES,
                            TimeUnit.MINUTES
                    );

            long cancelled = futures.stream()
                    .filter(Future::isCancelled)
                    .count();

            if (cancelled > 0) {
                log.warn(
                        "Очистка {} завершена по timeout. " +
                        "Не обработано: {}",
                        entityType,
                        cancelled
                );
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            log.error(
                    "Очистка {} была прервана",
                    entityType,
                    e
            );
        } finally {
            executor.shutdown();
        }
    }
}