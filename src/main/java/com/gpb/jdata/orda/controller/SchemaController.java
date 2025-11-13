package com.gpb.jdata.orda.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.orda.service.SchemaService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/schema")
@RequiredArgsConstructor
@Tag(name = "ОРДА. Схема базы данных", description = "API синхронизации схем базы данных")
public class SchemaController {
    
    private final SchemaService schemaService;
    private final SvoiCustomLogger logger;

    @PostMapping("/sync")
    @Operation(summary = "Запуск синхронизации схем базы данных")
    public ResponseEntity<Void> syncSchemas(HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "SyncSchema");
        schemaService.syncSchema();
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/delete/{fqn}")
    @Operation(summary = "Удаление схемы ао FQN")
    public ResponseEntity<Void> deleteSchema(@PathVariable String fqn, 
            HttpServletRequest httpServletRequest) {
        logger.logApiCall(httpServletRequest, "DeleteSchema");
        schemaService.deleteSchema(fqn);
        return ResponseEntity.ok().build();
    }
}
