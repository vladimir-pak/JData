package com.gpb.jdata.service;

import java.sql.SQLException;

public interface PGClassService {
    void initialSnapshot() throws SQLException;
    void synchronize() throws SQLException;
}
