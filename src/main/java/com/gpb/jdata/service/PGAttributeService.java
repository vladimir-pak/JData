package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.gpb.jdata.models.master.PGAttribute;

public interface PGAttributeService {
    void initialSnapshot() throws SQLException;
    CompletableFuture<Void> initialSnapshotAsync() throws SQLException;
    void synchronize() throws SQLException ;
    CompletableFuture<Void> synchronizeAsync() throws SQLException ;
    List<PGAttribute> readMasterData(Connection connection) throws SQLException;
}
