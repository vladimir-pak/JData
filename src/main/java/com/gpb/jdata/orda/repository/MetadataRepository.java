package com.gpb.jdata.orda.repository;

import java.util.List;

import com.gpb.jdata.orda.model.BaseEntity;

public interface MetadataRepository<T extends BaseEntity> {
    List<T> findByOid(Long oid);
}
