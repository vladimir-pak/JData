package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGNamespaceReplication;

@Repository
public interface PGNamespaceRepository 
        extends JpaRepository<PGNamespaceReplication, Long> {

    PGNamespaceReplication findPGNamespaceReplicationByOid(Long oid);
}
