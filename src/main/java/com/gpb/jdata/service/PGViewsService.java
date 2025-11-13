package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.gpb.jdata.models.master.PGViews;

public interface PGViewsService {
    List<PGViews> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
    CompletableFuture<Void> synchronizeAsync();
    List<PGViews> readMasterData(Connection connection) throws SQLException;
}
