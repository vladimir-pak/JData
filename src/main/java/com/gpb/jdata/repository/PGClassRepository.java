package com.gpb.jdata.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.gpb.jdata.models.replication.PGClassReplication;

@Repository
public interface PGClassRepository extends JpaRepository<PGClassReplication, Long> {

    PGClassReplication findPGClassReplicationByOid(Long oid);

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_class_rep_tmp m
            LEFT JOIN jdata.pg_class_rep r
                ON r.oid = m.oid
            WHERE r.oid IS NULL
            """, nativeQuery = true)
    List<PGClassReplication> findNew();

    @Query(value = """
            SELECT r.*
            FROM jdata.pg_class_rep r
            LEFT JOIN jdata.pg_class_rep_tmp m
                ON m.oid = r.oid
            WHERE m.oid IS NULL;
            """, nativeQuery = true)
    List<PGClassReplication> findDeleted();

    @Query(value = """
            SELECT m.*
            FROM jdata.pg_class_rep_tmp m
            JOIN jdata.pg_class_rep r
                ON m.oid = r.oid
            WHERE 
                (m.relname    IS DISTINCT FROM r.relname)
            OR (m.relnamespace IS DISTINCT FROM r.relnamespace)
            OR (m.relkind IS DISTINCT FROM r.relkind);
            """, nativeQuery = true)
    List<PGClassReplication> findUpdated();

    @Query("SELECT c.oid FROM PGClassReplication c WHERE c.relnamespace || '.' || c.relname = :fqn")
    Optional<Long> findOidByFqn(@Param("fqn") String fqn);

    @Transactional
    @Modifying
    @Query(value = "truncate table jdata.pg_class_rep_tmp", nativeQuery = true)
    void truncateTempTable();
}
