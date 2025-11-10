package com.gpb.jdata.orda.mapper;

import java.util.HashMap;
import java.util.Map;

public class SchemaMapper {
    public static Map<String, Object> toRequestBody(Map<String, Object> row) {
        Map<String, Object> body = new HashMap<>();
        body.put("database", "adb");
        String name = (String) row.get("schemaname");
        body.put("displayName", name);
        body.put("name", name);
        body.put("description", row.get("description"));
        return body;
    }
}
