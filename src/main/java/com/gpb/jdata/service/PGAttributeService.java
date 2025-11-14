package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.gpb.jdata.models.master.PGAttribute;

public interface PGAttributeService {
    void initialSnapshot() throws SQLException;
    CompletableFuture<Void> initialSnapshotAsync() throws SQLException;
    void synchronize();
    CompletableFuture<Void> synchronizeAsync();
    List<PGAttribute> readMasterData(Connection connection) throws SQLException;
}
