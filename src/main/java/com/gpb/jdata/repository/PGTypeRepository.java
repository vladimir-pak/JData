package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGTypeReplication;

@Repository
public interface PGTypeRepository extends JpaRepository<PGTypeReplication, Long> {

    PGTypeReplication findPGTypeReplicationByOid(Long oid);
}
