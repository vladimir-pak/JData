package com.gpb.jdata.orda.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.orda.service.TableService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/table")
@RequiredArgsConstructor
@Tag(name = "ОРДА. Таблица", description = "API синхронизации таблиц")
public class TableController {
    
    private final TableService tableService;
    private final SvoiCustomLogger logger;
    
    @PostMapping("/sync")
    @Operation(summary = "Запуск синхронизации таблиц")
    public ResponseEntity<Void> syncTables(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "SyncTable");
        tableService.syncTables();
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/delete/{fqn}")
    @Operation(summary = "Удаление таблицы по FQN")
    public ResponseEntity<Void> deleteTable(@PathVariable String fqn,
            HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "DeleteTable");
        tableService.deleteTable(fqn);
        return ResponseEntity.ok().build();
    }
}
