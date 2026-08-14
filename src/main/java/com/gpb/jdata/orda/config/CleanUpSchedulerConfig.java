package com.gpb.jdata.orda.config;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.properties.OrdCleanupProperties;
import com.gpb.jdata.orda.service.CleanUpService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class CleanUpSchedulerConfig {

    private final CleanUpService cleanUpService;
    private final OrdCleanupProperties ordCleanupProperties;

    /**
     * Метод запускает механизм очистки метаданных в OMD по заданному расписанию
     * Если предыдущая ещё не закончилась — просто пропускаем.
     */
    @Scheduled(cron = "${ord.cleanup.schedule}")
    public void synchronizeAll() {
        if (!ordCleanupProperties.isEnabled()) {
            log.debug("Очистка сущностей в OMD отключена");
            return;
        }

        log.info("=== Запуск чистки OMD по расписанию ===");

        boolean started =
                cleanUpService.start(ordCleanupProperties.isValidate());

        if (!started) {
            log.info("Очистка уже выполняется. Scheduled-запуск пропущен");
        }
    }
}
