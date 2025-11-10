package com.gpb.jdata.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.gpb.jdata.models.master.PGAttrdef;

public interface PGAttrdefService {
    List<PGAttrdef> initialSnapshot(Connection connection) throws SQLException;

    void synchronize();
}
