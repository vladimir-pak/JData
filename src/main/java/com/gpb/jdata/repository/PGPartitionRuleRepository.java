package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGPartitionRuleReplication;

@Repository
public interface PGPartitionRuleRepository 
        extends JpaRepository<PGPartitionRuleReplication, Long> {

}
