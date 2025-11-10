package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGPartitionReplication;

@Repository
public interface PGPartitionRepository 
        extends JpaRepository<PGPartitionReplication, Long> {

}
