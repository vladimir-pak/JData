package com.gpb.jdata.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.gpb.jdata.models.master.PGAttrdefId;
import com.gpb.jdata.models.replication.PGAttrdefReplication;

@Repository
public interface PGAttrdefRepository 
    extends JpaRepository<PGAttrdefReplication, PGAttrdefId> {

    PGAttrdefReplication findPGAttrdefReplicationById(PGAttrdefId id);

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_attrdef_rep_tmp m
            LEFT JOIN jdata.pg_attrdef_rep r
                ON r.adrelid = m.adrelid
               AND r.adnum  = m.adnum
            WHERE r.adrelid IS NULL
            """, nativeQuery = true)
    List<PGAttrdefReplication> findNew();

    @Query(value = """
            SELECT r.*
            FROM jdata.pg_attrdef_rep r
            LEFT JOIN jdata.pg_attrdef_rep_tmp m
                ON m.adrelid = r.adrelid
                AND m.adnum  = r.adnum
            WHERE m.adrelid IS NULL;
            """, nativeQuery = true)
    List<PGAttrdefReplication> findDeleted();

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_attrdef_rep_tmp m
            JOIN jdata.pg_attrdef_rep r
                ON m.adrelid = r.adrelid
                AND m.adnum  = r.adnum
            WHERE 
                (m.adbin IS DISTINCT FROM r.adbin);
            """, nativeQuery = true)
    List<PGAttrdefReplication> findUpdated();

    @Transactional
    @Modifying
    @Query(value = "truncate table jdata.pg_attrdef_rep_tmp", nativeQuery = true)
    void truncateTempTable();
}
