package com.gpb.jdata.orda.repository;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class TableRepository {

    private final JdbcTemplate jdbcTemplate;

    public TableRepository(@Qualifier("jdataDataSource") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> getTableByOid(Long oid) {
        String sql = """
            select nsp.nspname as dbschema,
                   cl.relname as tablename,
                   case when part.parrelid is not null then 'Partitioned'
                        when cl.relkind = 'r' then 'Regular'
                        when cl.relkind = 'v' then 'View'
                        when cl.relkind = 'M' then 'View'
                        when cl.relkind = 'f' then 'Foreign'
                        else null end as tabletype,
                   dt.description as table_description,
                   attr.attname as columnname,
                   tp.typname as dtype,
                   attr.atttypmod as dtlength,
                   attr.attnotnull as notnull,
                   attr.attnum as attnum,
                   dat.description as column_description,
                   case when con.contype = 'p' then 
                     (select array_agg(pka.attname) 
                      from jdata.pg_attribute_rep pka
                      where pka.attrelid = cl."oid"
                      and pka.attnum = any(con.conkey))  else null end as pk_constraint,
                   case when con.contype = 'f' then 
                     (select array_agg(fkn.nspname || '.' || fkc.relname || '.' || fka.attname)
                      from jdata.pg_class_rep fkc
                      join jdata.pg_namespace_rep fkn on fkc.relnamespace = fkn."oid" 
                      join jdata.pg_attribute_rep fka on fkc."oid" = fka.attrelid
                      where fkc."oid" = con.confrelid
                      and fka.attnum = any(con.confkey)) else null end as fk_constraint,
                   v.definition as view_definition
            from jdata.pg_class_rep cl
            join jdata.pg_namespace_rep nsp
              on cl.relnamespace = nsp."oid"
            left join jdata.pg_attribute_rep attr
              on cl."oid" = attr.attrelid
             and attr.attnum > 0
            left join jdata.pg_constraint_rep con
              on cl."oid" = con.conrelid
             and cl.relnamespace = con.connamespace
            left join jdata.pg_type_rep tp
              on attr.atttypid = tp."oid"
            left join jdata.pg_description_rep dt
              on dt.objoid = cl."oid"
             and dt.objsubid = 0
            left join jdata.pg_description_rep dat
              on dat.objoid = cl."oid"
             and dat.objsubid = attr.attnum
            left join jdata.pg_views_rep v
              on v.schemaname = nsp.nspname
             and v.viewname   = cl.relname
             and cl.relkind in ('m','v')
            left join jdata.pg_partition_rep part
              on part.parrelid = cl."oid"
            where cl.relkind in ('r','v','m','f','p')
              and cl."oid" = ?
        """;
        try {
            return jdbcTemplate.queryForList(sql, oid);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    }

    public List<Long> findAll() {
        String sql = """
            SELECT cl."oid"
            FROM jdata.pg_class_rep cl
        """;
        try {
            return jdbcTemplate.queryForList(sql, Long.class);
        } catch (EmptyResultDataAccessException e) {
            return null; // или throw new EntityNotFoundException("Schema not found with oid: " + oid);
        }
    } 

    public List<Map<String, Object>> getTableInfoById(int tableId) {
        String sql = """
            select nsp.nspname || '.' || cl.relname as tablename, att.attname , att.attnum
            from jdata.pg_class_rep cl
            join jdata.pg_namespace_rep nsp on cl.relnamespace = nsp.oid
            left join pg_attribute_rep att on cl."oid" = att.attrelid
            where cl.oid = ?
        """;
        return jdbcTemplate.queryForList(sql, tableId);
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
}
