package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGPartitionRule;

public interface PGPartitionRuleService {
    List<PGPartitionRule> initialSnapshot(Connection connection) throws SQLException;
    void replicate(List<PGPartitionRule> data, Connection connection) throws SQLException;
    void synchronize();
}
