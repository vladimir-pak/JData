package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGClass;

public interface PGClassService {
    void replicate(List<PGClass> data, Connection connection) throws SQLException;
    List<PGClass> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
}
