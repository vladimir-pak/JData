package com.gpb.jdata.orda.mapper;

import java.util.Map;

import com.gpb.jdata.orda.dto.SchemaDTO;

public class SchemaMapper {
    public static SchemaDTO toRequestBody(Map<String, Object> row, String database) {
        return SchemaDTO.builder()
                .name(row.get("schemaname").toString())
                .displayName(row.get("schemaname").toString())
                .description(row.get("description").toString())
                .database(database)
                .build();
    }
}
