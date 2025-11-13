package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.gpb.jdata.models.master.PGPartition;

public interface PGPartitionService {
    void replicate(List<PGPartition> data, Connection connection) throws SQLException;
    List<PGPartition> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
    CompletableFuture<Void> synchronizeAsync();
}
