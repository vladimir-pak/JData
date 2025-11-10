package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGPartitionedTableReplication;

@Repository
public interface PGPartitionedTableRepository 
        extends JpaRepository<PGPartitionedTableReplication, Long> {

}
