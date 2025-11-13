package com.gpb.jdata.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGClassReplication;

@Repository
public interface PGClassRepository extends JpaRepository<PGClassReplication, Long> {

    PGClassReplication findPGClassReplicationByOid(Long oid);

    @Query("SELECT c.oid FROM PGClassReplication c WHERE c.relnamespace || '.' || c.relname = :fqn")
    Optional<Long> findOidByFqn(@Param("fqn") String fqn);
}
