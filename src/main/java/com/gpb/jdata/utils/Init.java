package com.gpb.jdata.utils;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.service.MetadataService;
import com.gpb.jdata.properties.SyncProperties;
import com.gpb.jdata.service.PGAttributeService;
import com.gpb.jdata.service.PGClassService;
import com.gpb.jdata.service.PGConstraintService;
import com.gpb.jdata.service.PGDescriptionService;
import com.gpb.jdata.service.PGNamespaceService;
import com.gpb.jdata.service.PGPartitionRuleService;
import com.gpb.jdata.service.PGPartitionService;
import com.gpb.jdata.service.PGTypeService;
import com.gpb.jdata.service.PGViewsService;
import com.gpb.jdata.utils.diff.ClassDiffContainer;
import com.gpb.jdata.utils.diff.NamespaceDiffContainer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
public class Init {
    private static final Logger logger = LoggerFactory.getLogger(Init.class);

    private final AtomicBoolean running = new AtomicBoolean(false);

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
    private final SyncProperties syncProperties;

    private final MetadataService metadataService;

    private final ClassDiffContainer diffClassContainer;
    private final NamespaceDiffContainer diffNamespaceContainer;

    /**
     * Метод проверяет раз в interval миллисекунд, можно ли запустить синхронизацию.
     * Если предыдущая ещё не закончилась — просто пропускаем.
     */
    @Scheduled(fixedDelayString = "${sync.interval:60000}")
    public void synchronizeAll() {
        if (!syncProperties.isEnabled()) {
            logger.debug("Синхронизация отключена (sync.enabled=false)");
            return;
        }

        if (!running.compareAndSet(false, true)) {
            logger.debug("Предыдущая синхронизация ещё выполняется — пропускаем запуск");
            return;
        }

        try {
            logger.info("=== Начало полной синхронизации ===");
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

            metadataService.syncMetadata();

            logger.info("=== Полная синхронизация завершена ===");
        } catch (Exception e) {
            logger.error("Ошибка при синхронизации", e);
        } finally {
            running.set(false);
        }
    }
}
