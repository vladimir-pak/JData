package com.gpb.jdata.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.gpb.jdata.models.master.PGSequence;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGSequenceReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGSequenceRepository;
import com.gpb.jdata.service.PGSequenceService;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@Transactional
@RequiredArgsConstructor
public class PGSequenceServiceImpl implements PGSequenceService {
    private static final Logger logger = LoggerFactory.getLogger(PGSequenceServiceImpl.class);

    private final PGSequenceRepository pgSequenceRepository;
    private final ActionRepository actionRepository;
    private final DataSource dataSource;
    private final SessionFactory postgreSessionFactory;

    private long lastTransactionCount = 0;

    @Override
    public List<PGSequence> initialSnapshot(Connection connection) throws SQLException {
        List<PGSequence> result = new ArrayList<>();
        String sql = """
                SELECT seqrelid, seqstart, seqincrement, seqmin, seqmax, seqcache, seqcycle
                FROM pg_catalog.pg_sequence
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                PGSequence entity = new PGSequence(
                        resultSet.getLong("seqrelid"),
                        resultSet.getBigDecimal("seqstart"),
                        resultSet.getBigDecimal("seqincrement"),
                        resultSet.getBigDecimal("seqmin"),
                        resultSet.getBigDecimal("seqmax"),
                        resultSet.getBigDecimal("seqcache"),
                        resultSet.getBoolean("seqcycle")
                );
                result.add(entity);
            }
        }
        logger.info("[pg_sequence] Initial snapshot created: {} records", result.size());
        replicate(result, connection);
        writeStatistics((long) result.size(), "pg_sequence", connection);
        logAction("INITIAL_SNAPSHOT", "pg_sequence", result.size() 
                + " records added", "");
        return result;
    }

    @Override
    public void replicate(List<PGSequence> data, Connection connection) throws SQLException {
        String dbName = extractDbName(connection);
        List<PGSequenceReplication> replicationList = new ArrayList<>();
        for (PGSequence item : data) {
            replicationList.add(convertToReplication(item, dbName));
        }
        pgSequenceRepository.saveAll(replicationList);
        logger.info("[pg_sequence_rep] Replicated {} records successfully", replicationList.size());
        writeStatistics((long) replicationList.size(), "pg_sequence_rep", connection);
    }

    @Override
    public void synchronize() {
        try (Connection connection = dataSource.getConnection()) {
            long currentTransactionCount = getTransactionCount(connection);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_sequence] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }
            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_sequence] {} Changes detected. Starting synchronization...", diff);
            List<PGSequence> newData = initialSnapshot(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_sequence_rep", connection);
        } catch (SQLException e) {
            logger.error("[pg_sequence] Error during synchronization", e);
        }
    }

    private void compareSnapshots(List<PGSequence> newData, Connection connection) throws SQLException {
        List<PGSequenceReplication> existing = pgSequenceRepository.findAll();
        Map<Long, PGSequenceReplication> existingMap = new HashMap<>();
        for (PGSequenceReplication rep : existing) {
            existingMap.put(rep.getSeqrelid(), rep);
        }
        List<PGSequence> toAdd = new ArrayList<>();
        List<PGSequence> toUpdate = new ArrayList<>();
        Set<Long> toDelete = new HashSet<>(existingMap.keySet());

        for (PGSequence item : newData) {
            PGSequenceReplication match = existingMap.get(item.getSeqrelid());
            if (match == null) {
                toAdd.add(item);
            } else {
                if (!convertToReplication(item, match.getDb()).equals(match)) {
                    toUpdate.add(item);
                }
                toDelete.remove(item.getSeqrelid());
            }
        }

        if (!toDelete.isEmpty()) {
            logger.info("[pg_sequence_rep] Deleting {} records from replica table", toDelete.size());
            pgSequenceRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_sequence_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_sequence_rep] Adding {} records to replica table", toAdd.size());
            replicate(toAdd, connection);
            logAction("INSERT", "pg_sequence_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_sequence_rep] Updating {} records in replica table", toUpdate.size());
            toUpdate.forEach(e -> {
                PGSequenceReplication old = existingMap.get(e.getSeqrelid());
                logAction("UPDATE", "pg_sequence_rep",
                        "old: " + (old != null ? old.toString() : "null"),
                        "new: " + e.toString());
            });
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_sequence_rep", toUpdate.size() 
                    + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_sequence_rep", connection);
    }

    private PGSequenceReplication convertToReplication(PGSequence item, String db) {
        return new PGSequenceReplication(
                item.getSeqrelid(),
                item.getSeqstart(),
                item.getSeqincrement(),
                item.getSeqmin(),
                item.getSeqmax(),
                item.getSeqcache(),
                item.getSeqcycle(),
                db
        );
    }

    private String extractDbName(Connection connection) throws SQLException {
        String url = connection.getMetaData().getURL();
        return url.substring(url.lastIndexOf("/") + 1);
    }

    private long getTransactionCount(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = ? AND relname = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, "pg_catalog");
            ps.setString(2, "pg_sequence");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong("count");
            }
        }
        throw new SQLException("Failed to retrieve transaction count for pg_sequence");
    }

    private void writeStatistics(Long operations, String tableName, Connection connection) throws SQLException {
        try (Session session = postgreSessionFactory.openSession()) {
            session.beginTransaction();
            Statistics stat = new Statistics();
            String dbName = extractDbName(connection);
            stat.setDb(dbName);
            stat.setSchema("pg_catalog");
            stat.setTable_name(tableName);
            stat.setSum(operations);
            stat.setTimestamp(new Timestamp(System.currentTimeMillis()));
            session.persist(stat);
            session.getTransaction().commit();
        }
        logger.info("[pg_sequence] Statistics written for table {}", tableName);
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
