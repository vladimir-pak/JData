package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGDatabase;

public interface PGDatabaseService {
    List<PGDatabase> initialSnapshot(Connection connection) throws SQLException;
    void replicate(List<PGDatabase> data, Connection connection) throws SQLException;
    void synchronize();
}
