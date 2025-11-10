package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.master.PGViewsId;
import com.gpb.jdata.models.replication.PGViewsReplication;

@Repository
public interface PGViewsRepository 
        extends JpaRepository<PGViewsReplication, PGViewsId> {

}
