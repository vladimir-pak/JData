package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.master.PGAttributeId;
import com.gpb.jdata.models.replication.PGAttributeReplication;

@Repository
public interface PGAttributeRepository 
        extends JpaRepository<PGAttributeReplication, PGAttributeId> {
            
    PGAttributeReplication findPGAttributeReplicationById(PGAttributeId id);
}
