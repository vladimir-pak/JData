package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGClassReplication;

@Repository
public interface PGClassRepository extends JpaRepository<PGClassReplication, Long> {

    PGClassReplication findPGClassReplicationByOid(Long oid);
}
