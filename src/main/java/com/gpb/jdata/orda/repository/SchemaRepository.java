package com.gpb.jdata.orda.repository;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.orda.dto.SchemaDTO;
import com.gpb.jdata.orda.mapper.SchemaRowMapper;

@Repository
public class SchemaRepository {

    private final JdbcTemplate jdbcTemplate;

    public SchemaRepository(@Qualifier("jdataDataSource") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public SchemaDTO getSchemaByOid(Long oid) {
        String sql = """
            SELECT sch."oid", sch.nspname as name, dsc.description
            FROM jdata.pg_namespace_rep sch
            LEFT JOIN jdata.pg_description_rep dsc
                ON sch."oid" = dsc.objoid
                AND dsc.objsubid = 0
            WHERE sch."oid" = ?
        """;
        try {
            return jdbcTemplate.queryForObject(sql, new SchemaRowMapper(), oid);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    }

    public List<Long> findAll() {
        String sql = """
            SELECT sch."oid"
            FROM jdata.pg_namespace_rep sch
        """;
        try {
            return jdbcTemplate.queryForList(sql, Long.class);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    } 

    public String getSchemaNameById(int schemaId) {
        String sql = "SELECT nspname FROM jdata.pg_namespace_rep WHERE oid = ?";
        return jdbcTemplate.queryForObject(sql, String.class, schemaId);
    }
}
