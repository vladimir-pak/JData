package com.gpb.jdata.utils.pconnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Data;

@Data
public class PostgreSQLConnector {
    static Logger logger = LoggerFactory.getLogger(PostgreSQLConnector.class);

    public Connection connect(DBConnector connector) throws SQLException {
        Connection conn = DriverManager.getConnection(connector.getUrl(), connector.getUsername(),
                connector.getPassword());
        return conn;
    }

    public Long getSum(Connection conn, String schemaName, String tableName) throws SQLException, IOException {
        InputStream is = PostgreSQLConnector.class.getResourceAsStream("/PostgreSqlOperations.sql");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String query = "";
        query = reader.readLine();
        logger.info(query);
        query = format(query, new String[] { schemaName, tableName });
        int retval = 0;
        try (Statement stmt = conn.createStatement()) {
            logger.info(query);
            logger.info(conn.getMetaData().toString());
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                retval = rs.getInt(1);
            }
            stmt.close();
            rs.close();
        }
        return Long.valueOf(retval);
    }

    public boolean createTable(Connection conn, String tableName) throws IOException, SQLException {
        if (!tableExists(conn, tableName)) {
            String file = tableName + ".sql";
            Path path = Paths.get("/app/java_snapshot", file);
            String query = Files.readString(path);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(query);
                stmt.close();
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData dbm = conn.getMetaData();
        ResultSet rs = dbm.getTables(null, null, tableName, null);
        if (rs.next()) {
            return true;
        }
        return false;
    }

    public String format(String query, Object[] args) {
        MessageFormat mf = new MessageFormat(query);
        String output = mf.format(args);
        return output;
    }
}
