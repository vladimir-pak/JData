package com.gpb.jdata.orda.repository;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.jdata.orda.dto.TableDTO;
import com.gpb.jdata.orda.dto.ViewDTO;
import com.gpb.jdata.orda.mapper.TableRowMapper;
import com.gpb.jdata.orda.mapper.ViewRowMapper;
import com.gpb.jdata.orda.properties.OrdProperties;

@Repository
public class TableRepository {
    
    @Qualifier("jdataDataSource")
    private final JdbcTemplate jdbcTemplate;

    private final OrdProperties properties;

    private final ObjectMapper objectMapper;
    
    public TableRepository(@Qualifier("jdataDataSource") JdbcTemplate jdbcTemplate, OrdProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
    }

    public TableDTO findByOid(Long oid) {
        String sql = """
            SELECT
                c.oid,
                n.nspname as schema_name,
                c.relname as table_name,
                d.description as description,
                jsonb_build_object(
                    'tableType',
                    CASE 
                        WHEN par."oid" is not null  THEN 'PARTITIONED'
                        WHEN c.relkind = 'r' THEN 'REGULAR'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                        WHEN c.relkind = 'p' THEN 'PARTITIONED'
                        WHEN c.relkind = 'f' THEN 'FOREIGN'
                        ELSE 'OTHER'
                    END,
                    'viewDefinition',
                    CASE 
                        WHEN c.relkind IN ('v', 'm') THEN 
                            (SELECT v.definition
                             FROM jdata.pg_views_rep v
                             WHERE v.schemaname = n.nspname
                             AND v.viewname   = c.relname)
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
                            'precision',
                                CASE
                                    WHEN a.atttypid = 1700 AND a.atttypmod > 0
                                        THEN ((a.atttypmod - 4) >> 16)
                                    ELSE NULL
                                END,
                            'scale',
                                CASE
                                    WHEN a.atttypid = 1700 AND a.atttypmod > 0
                                        THEN ((a.atttypmod - 4) & 65535)
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
                    AND con.contype IN ('p', 'u', 'f', 'c', 'x')),
                    'tablePartition',
                    (
                        SELECT jsonb_build_object(
                            -- список колонок, участвующих в партиционировании
                            'columns',
                            (
                                SELECT jsonb_agg(att.attname ORDER BY att.attnum)
                                FROM unnest(p.paratts::smallint[]) AS atnm
                                JOIN jdata.pg_attribute_rep att
                                ON att.attrelid = p.parrelid
                                AND att.attnum = atnm
                            ),
                            -- raw interval из pg_partition_rule_rep
                            'interval',
                            (
                                SELECT pr.parrangeevery
                                FROM jdata.pg_partition_rule_rep pr
                                WHERE pr.paroid = p.oid
                                AND pr.parrangeevery IS NOT NULL
                                ORDER BY pr.parruleord
                                LIMIT 1
                            ),
                            -- raw-вид партиционирования (R/L/…)
                            'partitionKind', p.parkind,
                            -- тип первой колонки партиционирования (для анализа в Java)
                            'partitionColumnType', pt.typname
                        )
                        FROM jdata.pg_partition_rep p
                        LEFT JOIN jdata.pg_attribute_rep a0
                            ON a0.attrelid = p.parrelid
                            AND a0.attnum   = p.paratts[1]
                        LEFT JOIN jdata.pg_type_rep pt
                            ON pt.oid = a0.atttypid
                        WHERE p.parrelid = c.oid
                        AND p.parlevel = 0
                        AND p.paristemplate = false
                        LIMIT 1
                    )
                ) as table_structure
            FROM jdata.pg_class_rep c
            JOIN jdata.pg_namespace_rep n ON n.oid = c.relnamespace
            LEFT JOIN jdata.pg_description_rep d ON d.objoid = c.oid
                AND d.objsubid = 0
            LEFT JOIN jdata.pg_partition_rep par ON c."oid" = par.parrelid
            	AND par.parlevel = 0
            	AND par.paristemplate = false
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

    public List<Long> findAll() {
        String sql = """
            SELECT cl."oid"
            FROM jdata.pg_class_rep cl
            WHERE cl.relkind in ('r','v','m','f','p')
        """;
        try {
            return jdbcTemplate.queryForList(sql, Long.class);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    }

    public Set<String> findAllTablesBySchema(String schema) {
        String sql = """
            select nsp.nspname || '.' || cl.relname as tablename
            from jdata.pg_class_rep cl
            join jdata.pg_namespace_rep nsp on cl.relnamespace = nsp.oid
            where nsp.nspname = ?
        """;
        try {
            List<String> names = jdbcTemplate.queryForList(sql, String.class, schema);
            return new HashSet<>(names);
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptySet();
        }
    }

    public ViewDTO findViewByOid(Long oid) {
        String sql = """
                select v.schemaname, v.viewname, v.definition
                from jdata.pg_class_rep cl
                join jdata.pg_namespace_rep nsp
                    on cl.relnamespace = nsp.oid
                join jdata.pg_views_rep v
                    on v.schemaname = nsp.nspname
                    and v.viewname = cl.relname
                where cl.oid = ?;
                """;
        try {
            return jdbcTemplate.queryForObject(sql, new ViewRowMapper(), oid);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
}
