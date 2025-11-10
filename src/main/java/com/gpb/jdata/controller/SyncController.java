package com.gpb.jdata.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.config.DatabaseConfig;
import com.gpb.jdata.service.PGAttrdefService;
import com.gpb.jdata.service.PGAttributeService;
import com.gpb.jdata.service.PGClassService;
import com.gpb.jdata.service.PGConstraintService;
import com.gpb.jdata.service.PGDescriptionService;
import com.gpb.jdata.service.PGNamespaceService;
import com.gpb.jdata.service.PGPartitionedTableService;
import com.gpb.jdata.service.PGTypeService;
import com.gpb.jdata.service.PGViewsService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Replication", description = "API запуска репликации")
public class SyncController {
    // private static final Logger logger = LoggerFactory.getLogger(SyncController.class);
    private final PGAttrdefService pgAttrdefService;
    private final PGAttributeService pgAttributeService;
    private final PGClassService pgClassService;
    private final PGConstraintService pgConstraintService;
    private final PGDescriptionService pgDescriptionService;
    private final PGNamespaceService pgNamespaceService;
    private final PGPartitionedTableService pgPartitionedTableService;
    private final PGTypeService pgTypeService;
    private final PGViewsService pgViewsService;
    private final DatabaseConfig databaseConfig;

    /**
     * Метод для выполнения начального снапшота всех таблиц.
     */
    @PostMapping("/initial-snapshot")
    @Operation(summary = "Выполнение начального снапшота всех таблиц")
    public ResponseEntity<Map<String, String>> initialSnapshot() throws SQLException {
        Map<String, String> response = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(
                databaseConfig.getUrl(),
                databaseConfig.getUsername(),
                databaseConfig.getPassword())) {
            pgClassService.initialSnapshot(connection);
            pgAttrdefService.initialSnapshot(connection);
            pgAttributeService.initialSnapshot(connection);
            pgConstraintService.initialSnapshot(connection);
            pgDescriptionService.initialSnapshot(connection);
            pgNamespaceService.initialSnapshot(connection);
            pgPartitionedTableService.initialSnapshot(connection);
            pgTypeService.initialSnapshot(connection);
            pgViewsService.initialSnapshot(connection);
            response.put("status", "success");
            response.put("message", "Initial snapshot completed successfully for all tables.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error during initial snapshot", e);
            response.put("status", "error");
            response.put("message", "Failed to create initial snapshot: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * Метод для выполнения периодической синхронизации всех таблиц.
     */
    @PostMapping("/synchronize")
    @Operation(summary = "Синхронизация всех таблиц")
    public ResponseEntity<Map<String, String>> synchronize() {
        Map<String, String> response = new HashMap<>();
        try {
            pgClassService.synchronize();
            pgAttrdefService.synchronize();
            pgConstraintService.synchronize();
            pgDescriptionService.synchronize();
            pgNamespaceService.synchronize();
            pgPartitionedTableService.synchronize();
            pgTypeService.synchronize();
            pgViewsService.synchronize();
            // pgAttributeServiceService.synchronize();
            response.put("status", "success");
            response.put("message", "Synchronization completed successfully for all tables.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error during synchronization", e);
            response.put("status", "error");
            response.put("message", "Failed to synchronize: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
