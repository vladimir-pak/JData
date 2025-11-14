package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGNamespace;

public interface PGNamespaceService {
    void initialSnapshot() throws SQLException;
    void synchronize();
    List<PGNamespace> readMasterData(Connection connection) throws SQLException;
}
