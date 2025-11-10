package com.gpb.jdata.orda.repository;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class SchemaRepository {

    private final JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getSchemas() {
        String sql = """
            select sch.nspname as schemaname, dsc.description
            from pg_namespace_rep sch
            left join pg_description_rep dsc
              on sch."oid" = dsc.objoid
             and dsc.objsubid = 0
        """;
        return jdbcTemplate.queryForList(sql);
    }

    public List<Map<String, Object>> getDeletedSchemas() {
        String sql = "SELECT nspname FROM pg_namespace_rep WHERE is_deleted = true";
        return jdbcTemplate.queryForList(sql);
    }

    public String getSchemaNameById(int schemaId) {
        String sql = "SELECT nspname FROM pg_namespace_rep WHERE oid = ?";
        return jdbcTemplate.queryForObject(sql, String.class, schemaId);
    }
}
