package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.master.PGDescriptionId;
import com.gpb.jdata.models.replication.PGDescriptionReplication;

@Repository
public interface PGDescriptionRepository 
        extends JpaRepository<PGDescriptionReplication, PGDescriptionId> {
    
    PGDescriptionReplication findPGDescriptionReplicationById(PGDescriptionId id);
}
