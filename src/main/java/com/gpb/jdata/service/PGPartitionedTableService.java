package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGPartitionedTable;

public interface PGPartitionedTableService {
    List<PGPartitionedTable> initialSnapshot(Connection connection) throws SQLException;
    void synchronize();
    List<PGPartitionedTable> readMasterData(Connection connection) throws SQLException;
}
