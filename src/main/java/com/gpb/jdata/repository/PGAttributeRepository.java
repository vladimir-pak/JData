package com.gpb.jdata.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.gpb.jdata.models.master.PGAttributeId;
import com.gpb.jdata.models.replication.PGAttributeReplication;

@Repository
public interface PGAttributeRepository 
        extends JpaRepository<PGAttributeReplication, PGAttributeId> {
            
    PGAttributeReplication findPGAttributeReplicationById(PGAttributeId id);

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_attribute_rep_tmp m
            LEFT JOIN jdata.pg_attribute_rep r
                ON r.attrelid = m.attrelid
               AND r.attname  = m.attname
            WHERE r.attrelid IS NULL
            """, nativeQuery = true)
    List<PGAttributeReplication> findNew();

    @Query(value = """
            SELECT r.*
            FROM jdata.pg_attribute_rep r
            LEFT JOIN jdata.pg_attribute_rep_tmp m
                ON m.attrelid = r.attrelid
                AND m.attname  = r.attname
            WHERE m.attrelid IS NULL;
            """, nativeQuery = true)
    List<PGAttributeReplication> findDeleted();

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_attribute_rep_tmp m
            JOIN jdata.pg_attribute_rep r
                ON m.attrelid = r.attrelid
                AND m.attname  = r.attname
            WHERE 
                (m.attnum    IS DISTINCT FROM r.attnum)
            OR (m.atthasdef IS DISTINCT FROM r.atthasdef)
            OR (m.attnotnull IS DISTINCT FROM r.attnotnull)
            OR (m.atttypid  IS DISTINCT FROM r.atttypid)
            OR (m.atttypmod IS DISTINCT FROM r.atttypmod);
            """, nativeQuery = true)
    List<PGAttributeReplication> findUpdated();

    @Transactional
    @Modifying
    @Query(value = "truncate table jdata.pg_attribute_rep_tmp", nativeQuery = true)
    void truncateTempTable();

}
