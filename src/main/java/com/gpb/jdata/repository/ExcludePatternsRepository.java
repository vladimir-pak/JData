package com.gpb.jdata.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ExcludePatternsRepository {

    @Qualifier("jdataDataSource")
    private final JdbcTemplate jdbcTemplate;

    public ExcludePatternsRepository(@Qualifier("jdataDataSource") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<String> getSchemaPatterns() {
        String sql = """
            SELECT pattern_expr
            FROM jdata.exclude_patterns
            WHERE entity_type = 'SCHEMA'
        """;
        try {
            return jdbcTemplate.queryForList(sql, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    }
}
