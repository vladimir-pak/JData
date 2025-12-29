package com.gpb.jdata.orda.repository;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.jdata.orda.mapper.TableRowMapper;
import com.gpb.jdata.orda.model.TableEntity;
import com.gpb.jdata.orda.properties.OrdProperties;

@Repository
public class TableJpaRepository {
    
    @Qualifier("jdataDataSource")
    private final JdbcTemplate jdbcTemplate;

    private final OrdProperties properties;

    private final ObjectMapper objectMapper;
    
    public TableJpaRepository(@Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate, OrdProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
    }

    public TableEntity findByOid(Long oid) {
        String sql = """
            SELECT
                c.oid,
                n.nspname as schema_name,
                c.relname as table_name,
                obj_description(c.oid, 'pg_class') as description,
                jsonb_build_object(
                    'tableType',
                    CASE 
                        WHEN c.relkind = 'r' THEN 'REGULAR'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                        WHEN c.relkind = 'p' THEN 'REGULAR'
                        ELSE 'OTHER'
                    END,
                    'viewDefinition',
                    CASE 
                        WHEN c.relkind IN ('v', 'm') THEN 
                            (SELECT pg_get_viewdef(c.oid, true))
                        ELSE NULL
                    END,
                    'columns', 
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'ordinalPosition', a.attnum,
                            'name', a.attname,
                            'dataType', replace(upper(split_part(format_type(a.atttypid, a.atttypmod), '(', 1)), ' ', '_'),
                            'dataTypeDisplay', format_type(a.atttypid, a.atttypmod),
                            'dataLength', 
                                CASE 
                                    WHEN a.atttypid IN (1042, 1043, 25) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END
                                    WHEN a.atttypid IN (1700) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN (a.atttypmod - 4) >> 16 ELSE NULL END
                                    WHEN a.atttypid IN (1083, 1114, 1184, 1266) THEN 
                                        CASE WHEN a.atttypmod > 0 THEN a.atttypmod & 65535 ELSE NULL END
                                    WHEN a.atttypid IN (1560, 1562) THEN 
                                        CASE WHEN a.atttypmod >= 0 THEN a.atttypmod ELSE NULL END
                                    ELSE NULL 
                                END,
                            'constraint', CASE WHEN a.attnotnull = true THEN 'NOT_NULL' ELSE null END,
                            'description', dat.description
                        ) ORDER BY a.attnum
                    )
                    FROM jdata.pg_attribute_rep a
                    left join jdata.pg_description_rep dat
			              on dat.objoid = a.attrelid
			             and dat.objsubid = a.attnum
                    WHERE a.attrelid = c.oid 
                    AND a.attnum > 0),
                    'tableConstraints',
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            'columns', (
                                SELECT jsonb_agg(a.attname)
                                FROM unnest(con.conkey) AS k(attnum)
                                JOIN jdata.pg_attribute_rep a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                            ),
                            'constraintType',
                            CASE 
                                WHEN con.contype = 'p' THEN 'PRIMARY_KEY'
                                WHEN con.contype = 'u' THEN 'UNIQUE'
                                WHEN con.contype = 'f' THEN 'FOREIGN_KEY'
                                WHEN con.contype = 'c' THEN 'CHECK'
                                WHEN con.contype = 'x' THEN 'EXCLUSION'
                                ELSE 'OTHER'
                            END
                        )
                    )
                    FROM jdata.pg_constraint_rep con
                    WHERE con.conrelid = c.oid
                    AND con.contype IN ('p', 'u', 'f', 'c', 'x'))
                ) as table_structure
            FROM jdata.pg_class_rep c
            JOIN jdata.pg_namespace_rep n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','v','m','f','p')
            AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            and c.oid = ?
            ORDER BY n.nspname, c.relname;
        """;
        try {
            return jdbcTemplate.queryForObject(sql, new TableRowMapper(properties, objectMapper), oid);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
}
