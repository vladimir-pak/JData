package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGAttribute;

public interface PGAttributeService {
    List<PGAttribute> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
    List<PGAttribute> readMasterData(Connection connection) throws SQLException;
}
