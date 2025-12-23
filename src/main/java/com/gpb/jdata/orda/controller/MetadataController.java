package com.gpb.jdata.orda.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.orda.service.MetadataService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/metadata")
@RequiredArgsConstructor
@Tag(name = "ОРДА. Все сущности", description = "API синхронизации всех сущностей")
public class MetadataController {
    
    private final MetadataService metadataService;
    private final SvoiCustomLogger logger;
    
    /**
     * @Полная синхронизация метаданных - POST /api/metadata/sync
     * @Проверка существования базы данных - GET /api/database/exists/adb
     * @Создание базы данных - POST /api/database/create/adb
     * @Синхронизация схем - POST /api/schema/sync
     * @Удаление схемы - DELETE /api/schema/delete/NPD_GP.adb.ndp_services
     * @Синхронизация таблиц - POST /api/table/sync/ndp_services
     * @Удаление таблицы - DELETE /api/table/delete/NPD_GP.adb.ndp_services.rrm_role_group_ad
     *
     */
    @PostMapping("/sync")
    @Operation(summary = "Запуск синхронизации всех сущностей")
    public ResponseEntity<Void> syncMetadata(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "SyncMetadata");
        metadataService.syncMetadata();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/init")
    @Operation(summary = "Инициализация всех сущностей")
    public ResponseEntity<Void> initMetadata(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "InitMetadata");
        metadataService.initialMetadata();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/ord/delete")
    @Operation(summary = "Удаление удаленных сущностей в ОРДе")
    public ResponseEntity<Void> deleteMetadataOrd(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "DeleteMetadata");
        metadataService.handleDeletionsInOrd();
        return ResponseEntity.ok().build();
    }
}
