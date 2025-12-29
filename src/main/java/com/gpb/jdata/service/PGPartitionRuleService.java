package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.gpb.jdata.models.master.PGPartitionRule;

public interface PGPartitionRuleService {
    void replicate(List<PGPartitionRule> data, Connection connection) throws SQLException;
    void initialSnapshot() throws SQLException;
    CompletableFuture<Void> initialSnapshotAsync() throws SQLException;
    void synchronize();
    CompletableFuture<Void> synchronizeAsync();
}
