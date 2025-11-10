package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.replication.PGSequenceReplication;

@Repository
public interface PGSequenceRepository
        extends JpaRepository<PGSequenceReplication, Long> {

}
