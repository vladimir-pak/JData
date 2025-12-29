package com.gpb.jdata.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.service.MetadataService;
import com.gpb.jdata.utils.diff.ClassDiffContainer;
import com.gpb.jdata.utils.diff.NamespaceDiffContainer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetadataSnaphostService {
    private final PGNamespaceService pgNamespaceService;
    private final PGClassService pgClassService;
    private final PGAttributeService pgAttributeService;
    // private final PGAttrdefService pgAttrdefService;
    private final PGConstraintService pgConstraintService;
    private final PGDescriptionService pgDescriptionService;
    private final PGTypeService pgTypeService;
    private final PGViewsService pgViewsService;
    private final PGPartitionService pgPartitionService;
    private final PGPartitionRuleService pgPartitionRuleService;
    private final PGDatabaseService pgDatabaseService;

    // private final MetadataService metadataService;

    private final ClassDiffContainer diffClassContainer;
    private final NamespaceDiffContainer diffNamespaceContainer;

    public void initAll() {
        try {
            log.info("=== Начало полной инициализации ===");
            diffClassContainer.clear();
            diffNamespaceContainer.clear();

            // Последовательно
            pgNamespaceService.initialSnapshot();
            pgClassService.initialSnapshot();
            pgAttributeService.initialSnapshot();
            // pgAttrdefService.initialSnapshot();

            // Параллельно
            CompletableFuture<Void> f1 = pgViewsService.initialSnapshotAsync();
            CompletableFuture<Void> f2 = pgDescriptionService.initialSnapshotAsync();
            CompletableFuture<Void> f3 = pgConstraintService.initialSnapshotAsync();
            CompletableFuture<Void> f4 = pgTypeService.initialSnapshotAsync();
            CompletableFuture<Void> f5 = pgPartitionService.initialSnapshotAsync();
            CompletableFuture<Void> f6 = pgDatabaseService.initialSnapshotAsync();
            CompletableFuture<Void> f7 = pgPartitionRuleService.initialSnapshotAsync();

            // Ждем завершения всех задач
            CompletableFuture.allOf(f1, f2, f3, f4, f5, f6, f7).join();

            // metadataService.syncMetadata();

            log.info("=== Полная инициализация завершена ===");
        } catch (Exception e) {
            log.error("Ошибка при инициализации", e);
        }
    }

    public void synchronizeAll() {
        try {
            log.info("=== Начало полной синхронизации ===");
            diffClassContainer.clear();
            diffNamespaceContainer.clear();

            // Последовательно
            pgNamespaceService.synchronize();
            pgClassService.synchronize();
            pgAttributeService.synchronize();
            // pgAttrdefService.synchronize();

            // Параллельно
            CompletableFuture<Void> f1 = pgViewsService.synchronizeAsync();
            CompletableFuture<Void> f2 = pgDescriptionService.synchronizeAsync();
            CompletableFuture<Void> f3 = pgConstraintService.synchronizeAsync();
            CompletableFuture<Void> f4 = pgTypeService.synchronizeAsync();
            CompletableFuture<Void> f5 = pgPartitionService.synchronizeAsync();
            CompletableFuture<Void> f6 = pgPartitionRuleService.synchronizeAsync();

            // Ждем завершения всех задач
            CompletableFuture.allOf(f1, f2, f3, f4, f5, f6).join();

            // metadataService.syncMetadata();

            log.info("=== Полная синхронизация завершена ===");
        } catch (Exception e) {
            log.error("Ошибка при синхронизации", e);
        }
    }
}
