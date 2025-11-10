package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGSequence;

public interface PGSequenceService {
    List<PGSequence> initialSnapshot(Connection connection) throws SQLException;
    void replicate(List<PGSequence> data, Connection connection) throws SQLException;
    void synchronize();
}
