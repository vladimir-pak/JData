package com.gpb.jdata.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGConstraintReplication;
import com.gpb.jdata.models.replication.PGNamespaceReplication;

@Repository
public interface PGConstraintRepository 
        extends JpaRepository<PGConstraintReplication, Long> {

    PGNamespaceReplication findPGNamespaceReplicationByOid(Long oid);

    List<PGConstraintReplication> findAll();

    void deleteAllById(Iterable<? extends Long> ids);
}
