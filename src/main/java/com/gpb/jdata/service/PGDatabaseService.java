package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.gpb.jdata.models.master.PGDatabase;

public interface PGDatabaseService {
    void initialSnapshot() throws SQLException;
    CompletableFuture<Void> initialSnapshotAsync() throws SQLException;
    void replicate(List<PGDatabase> data, Connection connection) throws SQLException;
    void synchronize();
}
