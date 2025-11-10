package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.master.PGAttrdefId;
import com.gpb.jdata.models.replication.PGAttrdefReplication;

@Repository
public interface PGAttrdefRepository 
        extends JpaRepository<PGAttrdefReplication, PGAttrdefId> {

}
