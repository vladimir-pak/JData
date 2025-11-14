package com.gpb.jdata.controller;

import java.sql.SQLException;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.service.MetadataSnaphostService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Replication", description = "API запуска репликации")
public class SyncController {
    private final MetadataSnaphostService metadataSnaphostService;

    private final SvoiCustomLogger logger;

    /**
     * Метод для выполнения начального снапшота всех таблиц.
     */
    @PostMapping("/initial-snapshot")
    @Operation(summary = "Выполнение начального снапшота всех таблиц")
    public ResponseEntity<Map<String, String>> initialSnapshot(HttpServletRequest httpServletRequest) 
            throws SQLException {
        logger.logApiCall(httpServletRequest, "StartInitialSnapshot");
        metadataSnaphostService.initAll();
        return ResponseEntity.ok().build();
    }

    /**
     * Метод для выполнения периодической синхронизации всех таблиц.
     */
    @PostMapping("/synchronize")
    @Operation(summary = "Синхронизация всех таблиц")
    public ResponseEntity<Map<String, String>> synchronize(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "StartSynchronizeGP");
        metadataSnaphostService.synchronizeAll();
        return ResponseEntity.ok().build();
    }
}
