package com.gpb.jdata.orda.repository;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.orda.properties.OrdProperties;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Repository
public class OpenMetadataRepository {

    private final OrdProperties ordProperties;
    private final JdbcTemplate ordJdbcTemplate;

    public OpenMetadataRepository(
            OrdProperties ordProperties,
            @Qualifier("ordJdbcTemplate") JdbcTemplate ordJdbcTemplate
    ) {
        this.ordProperties = ordProperties;
        this.ordJdbcTemplate = ordJdbcTemplate;
    }

    private static final String TABLE_SQL = """
            select te."json" ->> 'fullyQualifiedName' as fqn
            from table_entity te
            where deleted = false
              and coalesce(te."json" ->> 'isProjectEntity', 'false') != 'true'
              and te."json" -> 'database' ->> 'fullyQualifiedName' ilike ?
            """;

    private static final String SCHEMA_SQL = """
            select distinct se."json" ->> 'fullyQualifiedName' as fqn
            from database_schema_entity se
            left join (
                select er.fromid
                from entity_relationship er
                join table_entity te
                on er.toid = te.id
                and te.deleted = false
                and coalesce(te."json" ->> 'isProjectEntity', 'false') != 'true'
                where er.deleted = false
                and er.relation = 0
            ) t
            on se.id = t.fromid
            where se.deleted = false
            and se."json" -> 'database' ->> 'fullyQualifiedName' ilike ?
            and t.fromid is null
            """;

    public Set<String> findAllTables() {
        try {
            String servicePattern = ordProperties.getServiceName() + ".%";
            List<String> names = ordJdbcTemplate.queryForList(TABLE_SQL, String.class, servicePattern);
            return new HashSet<>(names);
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptySet();
        }
    }

    public Set<String> findAllSchemas() {
        try {
            String servicePattern = ordProperties.getServiceName() + ".%";
            List<String> names = ordJdbcTemplate.queryForList(SCHEMA_SQL, String.class, servicePattern);
            return new HashSet<>(names);
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptySet();
        }
    }
}
