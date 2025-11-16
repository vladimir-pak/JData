package com.gpb.jdata.orda.repository;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PartitionRepository {

    private final JdbcTemplate jdbcTemplate;

    public PartitionRepository(@Qualifier("jdataDataSource") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> getPartitions(String schemaName, String tableName) {
        String sql = """
            select
                ns.nspname as schema,
                par.relname as table_name,
                partition_strategy,
                col.attname
            from
                (select
                     parrelid,
                     parnatts,
                     case parkind
                          when 'l' then 'COLUMN-VALUE'
                          when 'h' then 'COLUMN-VALUE'
                          when 'r' then 'TIME-UNIT'
                     end as partition_strategy,
                     unnest(paratts) column_index
                 from
                     pg_partition_rep) pt
            join
                jdata.pg_class_rep par
              on par.oid = pt.parrelid
            left join
                jdata.pg_namespace_rep ns on par.relnamespace = ns.oid
            left join
                jdata.pg_attribute_rep col
              on col.attrelid = par."oid"
             and col.attnum   = pt.column_index
            where par.relname = ? and ns.nspname = ?
        """;
        return jdbcTemplate.queryForList(sql, tableName, schemaName);
    }
}
