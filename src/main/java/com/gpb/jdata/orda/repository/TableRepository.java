package com.gpb.jdata.orda.repository;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class TableRepository {

    private final JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getTables(String schemaName) {
        String sql = """
            select nsp.db as dbname,
                   nsp.nspname as dbschema,
                   dsch.description as dbschema_description,
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
                   case when con.contype = 'p' then con.conkey else null end as pk_constraint,
                   case when con.contype = 'f' then con.confrelid else null end as foreign_table_id,
                   case when con.contype = 'f' then con.confkey else null end as fk_constraint,
                   v.definition as view_definition
            from pg_class_rep cl
            join pg_namespace_rep nsp
              on cl.relnamespace = nsp."oid"
            left join pg_attribute_rep attr
              on cl."oid" = attr.attrelid
            left join pg_constraint_rep con
              on cl."oid" = con.conrelid
             and cl.relnamespace = con.connamespace
            left join pg_type_rep tp
              on attr.atttypid = tp."oid"
            left join pg_description_rep dsch
              on dsch.objoid = nsp."oid"
             and dsch.objsubid = 0
            left join pg_description_rep dt
              on dt.objoid = cl."oid"
             and dt.objsubid = 0
            left join pg_description_rep dat
              on dat.objoid = cl."oid"
             and dat.objsubid = attr.attnum
            left join pg_views_rep v
              on v.schemaname = nsp.nspname
             and v.viewname   = cl.relname
             and cl.relkind in ('m','v')
            left join pg_partition_rep part
              on part.parrelid = cl."oid"
            where cl.relkind in ('r','v','m','f','p')
              and nsp.nspname = ?
        """;
        return jdbcTemplate.queryForList(sql, schemaName);
    }

    public List<Map<String, Object>> getAllTables() {
        String sql = """
            select nsp.db as dbname,
                   nsp.nspname as dbschema,
                   dsch.description as dbschema_description,
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
                   case when con.contype = 'p' then con.conkey else null end as pk_constraint,
                   case when con.contype = 'f' then con.confrelid else null end as foreign_table_id,
                   case when con.contype = 'f' then con.confkey else null end as fk_constraint,
                   v.definition as view_definition
            from pg_class_rep cl
            join pg_namespace_rep nsp
              on cl.relnamespace = nsp."oid"
            left join pg_attribute_rep attr
              on cl."oid" =attr.attrelid
            left join pg_constraint_rep con
              on cl."oid" = con.conrelid
             and cl.relnamespace = con.connamespace
            left join pg_type_rep tp
              on attr.atttypid = tp."oid"
            left join pg_description_rep dsch
              on dsch.objoid = nsp."oid"
             and dsch.objsubid = 0
            left join pg_description_rep dt
              on dt.objoid = cl."oid"
             and dt.objsubid = 0
            left join pg_description_rep dat
              on dat.objoid = cl."oid"
             and dat.objsubid = attr.attnum
            left join pg_views_rep v
              on v.schemaname = nsp.nspname
             and v.viewname   = cl.relname
             and cl.relkind in ('m','v')
            left join pg_partition_rep part
              on part.parrelid = cl."oid"
            where cl.relkind in ('r','v','m','f','p')
        """;
        return jdbcTemplate.queryForList(sql);
    }

    public Map<String, Object> getTableInfoById(int tableId) {
        String sql = """
            select nsp.nspname as schemaname, cl.relname as tablename
            from pg_class_rep cl
            join pg_namespace_rep nsp on cl.relnamespace = nsp.oid
            where cl.oid = ?
        """;
        return jdbcTemplate.queryForMap(sql, tableId);
    }
    
    public List<Map<String, Object>> getDeletedTables() {
        String sql = """
            select nsp.nspname as schemaname, cl.relname as tablename
            from pg_class_rep cl
            join pg_namespace_rep nsp on cl.relnamespace = nsp.oid
            where cl.is_deleted = true
        """;
        return jdbcTemplate.queryForList(sql);
    }
}
