package com.gpb.jdata.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.gpb.jdata.models.replication.PGTypeReplication;

@Repository
public interface PGTypeRepository extends JpaRepository<PGTypeReplication, Long> {

    PGTypeReplication findPGTypeReplicationByOid(Long oid);

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_type_rep_tmp m
            LEFT JOIN jdata.pg_type_rep r
                ON r.oid = m.oid
            WHERE r.oid IS NULL
            """, nativeQuery = true)
    List<PGTypeReplication> findNew();

    @Query(value = """
            SELECT r.*
            FROM jdata.pg_type_rep r
            LEFT JOIN jdata.pg_type_rep_tmp m
                ON m.oid = r.oid
            WHERE m.oid IS NULL;
            """, nativeQuery = true)
    List<PGTypeReplication> findDeleted();

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_type_rep_tmp m
            JOIN jdata.pg_type_rep r
                ON m.oid = r.oid
            WHERE 
                (m.typname    IS DISTINCT FROM r.typname)
            OR (m.typnamespace IS DISTINCT FROM r.typnamespace);
            """, nativeQuery = true)
    List<PGTypeReplication> findUpdated();

    @Transactional
    @Modifying
    @Query(value = "truncate table jdata.pg_type_rep_tmp", nativeQuery = true)
    void truncateTempTable();
}
