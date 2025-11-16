package com.gpb.jdata.orda.mapper;

import java.util.Map;

import com.gpb.jdata.orda.dto.SchemaDTO;

public class SchemaMapper {
    public static SchemaDTO toRequestBody(Map<String, Object> row, String database) {
        String schemaname = row.get("schemaname") != null ? row.get("schemaname").toString() : null;
        String description = row.get("description") != null ? row.get("description").toString() : null;
        return SchemaDTO.builder()
                .name(schemaname)
                .displayName(schemaname)
                .description(description)
                .database(database)
                .build();
    }
}
