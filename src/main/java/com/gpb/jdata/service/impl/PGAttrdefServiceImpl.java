package com.gpb.jdata.service.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.gpb.jdata.config.DatabaseConfig;
import com.gpb.jdata.config.PersistanceTransactions;
import com.gpb.jdata.config.PersistanceTransactions.PgKey;
import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.log.SvoiSeverityEnum;
import com.gpb.jdata.models.master.PGAttrdefId;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGAttrdefReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGAttrdefRepository;
import com.gpb.jdata.service.PGAttrdefService;
import com.gpb.jdata.utils.PostgresCopyStreamer;
import com.gpb.jdata.utils.diff.ClassDiffContainer;

import lombok.RequiredArgsConstructor;

/**
pg_attrdef service provides creation of master data from pg_catalog.pg_attrdef
and inserting, deleting or updating it in the replica tables.
 */
@Service
@RequiredArgsConstructor
public class PGAttrdefServiceImpl implements PGAttrdefService {

    private static final Logger logger = LoggerFactory.getLogger(PGAttrdefService.class);
    private final SvoiCustomLogger svoiLogger;

    private final ActionRepository actionRepository;
    private final PGAttrdefRepository pgAttrdefRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final ClassDiffContainer diffContainer;

    private final PostgresCopyStreamer postgresCopyStreamer;

    private final static String masterQuery = """
                    SELECT attrelid, attnum, attname, atthasdef, attnotnull, atttypid, atttypmod
                    FROM pg_catalog.pg_attrdef
                    """;
    private final static String initialCopySql = """
                    COPY jdata.pg_attrdef_rep (
                        attrelid, attnum, attname, atthasdef,
                        attnotnull, atttypid, atttypmod
                    )
                    FROM STDIN WITH (FORMAT text)
                    """;
    private final static String copySql = """
                    COPY jdata.pg_attrdef_rep_tmp (
                        attrelid, attnum, attname, atthasdef,
                        attnotnull, atttypid, atttypmod
                    )
                    FROM STDIN WITH (FORMAT text)
                    """;

    private final PersistanceTransactions transactions;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
                "startInitSnapshot",
                "Start PGAttrdef init",
                "Started PGAttrdef init",
                SvoiSeverityEnum.ONE);

        try (Connection connection = databaseConfig.getConnection()) {
            logger.info("[pg_attrdef] Starting initial snapshot (streaming COPY)...");
            
            svoiLogger.logConnectToSource();

            long inserted = postgresCopyStreamer.streamCopy(
                    masterQuery, this::serializeRowFromResultSet, initialCopySql);

            writeStatistics(inserted, "pg_attrdef_rep", connection);
            logAction("INITIAL_SNAPSHOT", "pg_attrdef", 
                    inserted + " records added", "");

            logger.info("[pg_attrdef] Initial snapshot finished. Inserted: {}", inserted);
        } catch (SQLException | IOException e) {
            logger.error("[pg_attrdef] Error during initial snapshot", e);
            svoiLogger.logDbConnectionError(e);
            throw (e instanceof SQLException) ? (SQLException) e : 
                    new SQLException("Initial snapshot failed", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> initialSnapshotAsync() throws SQLException {
        initialSnapshot();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Синхронизация данных
     */
    @Override
    public void synchronize() throws SQLException {
        svoiLogger.send(
			"startSync", 
			"Start PGAttrdef sync", 
			"Started PGAttrdef sync", 
			SvoiSeverityEnum.ONE);

        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            long currentTransactionCount = getTransactionCount(connection);

            long lastTransactionCount = transactions.get(PgKey.PG_ATTRDEF);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_attrdef] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_attrdef] {} Changes detected. Starting synchronization...", diff);
            pgAttrdefRepository.truncateTempTable();
            postgresCopyStreamer.streamCopy(
                    masterQuery, this::serializeRowFromResultSet, copySql);

            compareSnapshots();
            transactions.put(PgKey.PG_ATTRDEF, currentTransactionCount);
            writeStatistics(currentTransactionCount, "pg_attrdef", connection);
        } catch (SQLException | IOException e) {
            logger.error("[pg_attrdef] Error during synchronization", e);
            svoiLogger.logDbConnectionError(e);
            throw (e instanceof SQLException) ? (SQLException) e : 
                    new SQLException("Synchronization failed", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> synchronizeAsync() throws SQLException {
        synchronize();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Сериализация одной строки для COPY в текстовом формате (TAB-separated).
     * Формат: col1\tcol2\t...\tcolN\n
     * NULL -> \N
     * boolean -> t/f
     */
    private String serializeRowFromResultSet(ResultSet rs) {
        try {
            long adrelid = rs.getLong("adrelid");
            long adnum = rs.getLong("adnum");
            String adbin = rs.getString("adbin");

            StringBuilder sb = new StringBuilder(128);
            sb.append(escape(adrelid)).append('\t');
            sb.append(escape(adnum)).append('\t');
            sb.append(escape(adbin)).append('\n');

            return sb.toString();
        } catch (SQLException e) {
            logger.error("Error serializing ResultSet row", e);
            throw new RuntimeException(e);
        }
    }

    private String escape(Object v) {
        if (v == null) return "\\N";
        String s = String.valueOf(v);
        // Простейшее экранирование для COPY text
        return s.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots() throws SQLException {
        List<PGAttrdefReplication> toAdd = pgAttrdefRepository.findNew();
        List<PGAttrdefReplication> toUpdate = pgAttrdefRepository.findUpdated();
        List<PGAttrdefReplication> toDelete = pgAttrdefRepository.findDeleted();
        List<PGAttrdefId> idsToDelete = toDelete.stream()
                .map(r -> r.getId())
                .collect(Collectors.toList());

        if (!toDelete.isEmpty()) {
            logger.info("[pg_attrdef_rep] Deleting {} records from the replica table", toDelete.size());
            pgAttrdefRepository.deleteAllById(idsToDelete);
            idsToDelete.forEach(e -> {
                    if (!diffContainer.containsInDeletedOids(e.getAdrelid())) {
                        diffContainer.addUpdated(e.getAdrelid());
                    }
            });
            logAction("DELETE", "pg_attrdef_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_attrdef_rep] Adding {} records to the replica table", toAdd.size());
            pgAttrdefRepository.saveAll(toAdd);
            toAdd.forEach(e -> diffContainer.addUpdated(e.getId().getAdrelid()));
            logAction("INSERT", "pg_attrdef_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_attrdef_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e -> {
                    diffContainer.addUpdated(e.getId().getAdrelid());
            });
            pgAttrdefRepository.saveAll(toUpdate);
            logAction("UPDATE", "pg_attrdef_rep", toUpdate.size() 
                    + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        try (Connection connection = databaseConfig.getConnection()) {
            writeStatistics(totalOperations, "pg_attrdef_rep", connection);
        }
    }

    /**
     * Получение количества операций для таблицы pg_attrdef
     */
    private long getTransactionCount(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = ? AND relname = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_attrdef");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong("count");
            }
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    /**
     * Запись статистики
     */
    private void writeStatistics(Long count, String tableName, Connection connection) {
        try (Session session = postgreSessionFactory.openSession()) {
            session.beginTransaction();
            Statistics stat = new Statistics();
            stat.setDb("pg_attrdef_db");
            stat.setSchema("pg_catalog");
            stat.setTable_name(tableName);
            stat.setSum(count);
            stat.setTimestamp(new Timestamp(System.currentTimeMillis()));
            session.persist(stat);
            session.getTransaction().commit();
            }
        logger.info("[pg_attrdef] Statistics written for table {}", tableName);
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
