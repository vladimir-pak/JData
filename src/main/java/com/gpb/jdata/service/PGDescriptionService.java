package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGDescription;

public interface PGDescriptionService {
    List<PGDescription> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
    List<PGDescription> readMasterData(Connection connection) throws SQLException;
}