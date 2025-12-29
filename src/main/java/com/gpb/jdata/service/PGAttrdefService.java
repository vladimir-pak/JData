package com.gpb.jdata.service;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public interface PGAttrdefService {
    void initialSnapshot() throws SQLException;
    CompletableFuture<Void> initialSnapshotAsync() throws SQLException;
    void synchronize() throws SQLException;
    CompletableFuture<Void> synchronizeAsync() throws SQLException ;
}
