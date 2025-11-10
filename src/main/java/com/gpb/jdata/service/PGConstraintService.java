package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGConstraint;

public interface PGConstraintService {
    List<PGConstraint> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
    List<PGConstraint> readMasterData(Connection connection) throws SQLException;
}
