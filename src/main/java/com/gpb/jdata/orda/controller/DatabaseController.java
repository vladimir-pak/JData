package com.gpb.jdata.orda.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.orda.service.DatabaseService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/database")
@RequiredArgsConstructor
@Tag(name = "ОРДА. База данных", description = "API создания базы данных")
public class DatabaseController {
    
    private final DatabaseService databaseService;
    private final SvoiCustomLogger logger;
    
    @GetMapping("/check-and-create")
    @Operation(summary = "Создание базы данных")
    public ResponseEntity<String> checkAndCreateDatabase(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "CreateDatabase");
        try {
            databaseService.checkAndCreateDatabase();
            return ResponseEntity.ok("Database 'adb' is ready.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/current-database")
    @Operation(summary = "Получение текущей базы данных")
    public ResponseEntity<String> getCurrentDatabaseName(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "GetDatabase");
        try {
            String databaseName = databaseService.getCurrentDatabaseName();
            return ResponseEntity.ok("Current database: " + databaseName);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error: " + e.getMessage());
        }
    }
}
