package com.gpb.jdata.service.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.gpb.jdata.config.DatabaseConfig;
import com.gpb.jdata.models.master.PGDatabase;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGDatabaseReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGDatabaseRepository;
import com.gpb.jdata.service.PGDatabaseService;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
@Deprecated
public class PGDatabaseServiceImpl implements PGDatabaseService {
    private static final Logger logger = LoggerFactory.getLogger(PGDatabaseServiceImpl.class);
    
    private final PGDatabaseRepository pgDatabaseRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    @Override
    @Transactional
    public List<PGDatabase> initialSnapshot(Connection connection) throws SQLException {
        List<PGDatabase> data = readMasterData(connection);
        replicate(data, connection);
        writeStatistics((long) data.size(), "pg_database", connection);
        logAction("INITIAL_SNAPSHOT", "pg_database", data.size() + " records added", "");
        return data;
    }

    @Override
    public void synchronize() {
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCount(connection);
            List<PGDatabase> newData = readMasterData(connection);
            compareSnapshots(newData, connection);

            writeStatistics(currentTransactionCount, "pg_database", connection);
        } catch (SQLException e) {
            logger.error("[pg_database] Error during synchronization", e);
        }
    }

    private List<PGDatabase> readMasterData(Connection connection) throws SQLException {
        List<PGDatabase> data = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT oid, datname FROM pg_catalog.pg_database")) {
            while (rs.next()) {
                data.add(new PGDatabase(
                        rs.getLong("oid"),
                        rs.getString("datname")
                ));
            }
        }
        return data;
    }

    @Override
    public void replicate(List<PGDatabase> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);
        String finalDb = db;
        List<PGDatabaseReplication> replicated = data.stream()
                .map(r -> new PGDatabaseReplication(r.getOid(), r.getDatname(), finalDb))
                .collect(Collectors.toList());

        if (replicated != null && !replicated.isEmpty()) {
            pgDatabaseRepository.saveAll(replicated);
            logger.info("[pg_database_rep] Replication complete.");
            writeStatistics((long) replicated.size(), "pg_database_rep", connection);
        } else {
            logger.info("[pg_database_rep] Data is empty.");
        }
    }

    private void compareSnapshots(List<PGDatabase> newData, Connection connection) throws SQLException {
        List<PGDatabaseReplication> replica = pgDatabaseRepository.findAll();
        Map<Long, PGDatabaseReplication> replicaMap = replica.stream()
                .collect(Collectors.toMap(PGDatabaseReplication::getOid, r -> r));

        List<PGDatabase> toAdd = new ArrayList<>();
        List<PGDatabase> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicaMap.keySet());

        for (PGDatabase master : newData) {
            PGDatabaseReplication replicaRow = replicaMap.get(master.getOid());
            if (replicaRow == null) {
                toAdd.add(master);
            } else {
                if (!convertToReplication(master, replicaRow.getDb()).equals(replicaRow)) {
                    toUpdate.add(master);
                }
                toDelete.remove(master.getOid());
            }
        }

        if (!toDelete.isEmpty()) {
            pgDatabaseRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_database_rep", toDelete.size() + " records deleted", "");
        }
        
        if (!toAdd.isEmpty()) {
            replicate(toAdd, connection);
            logAction("INSERT", "pg_database_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_database_rep", toUpdate.size() + " records updated", "");
        }

    }

    private PGDatabaseReplication convertToReplication(PGDatabase db, String dbName) {
        return new PGDatabaseReplication(db.getOid(), db.getDatname(), dbName);
    }

    private long getTransactionCount(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = 'pg_catalog' AND relname = 'pg_database'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) return rs.getLong("count");
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    private void writeStatistics(Long ops, String table, Connection connection) throws SQLException {
        try (Session session = postgreSessionFactory.openSession()) {
            session.beginTransaction();
            Statistics stat = new Statistics();
            String db = connection.getMetaData().getURL();
            db = db.substring(db.lastIndexOf("/") + 1);
            stat.setDb(db);
            stat.setSchema("pg_catalog");
            stat.setTable_name(table);
            stat.setSum(ops);
            stat.setTimestamp(new Timestamp(System.currentTimeMillis()));
            session.persist(stat);
            session.getTransaction().commit();
        }
        logger.info("[pg_database] Stats recorded.");
    }

    private void logAction(String actionType, String tableName, String description, String details) {
        Action action = new Action();
        action.setOperationType(actionType);
        action.setEntityName(tableName);
        action.setAdditionalInfo(description);
        action.setDetails(details);
        action.setTimestamp(LocalDateTime.now());
        actionRepository.save(action);
    }
}
