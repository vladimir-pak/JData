package com.gpb.jdata.service.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.gpb.jdata.config.DatabaseConfig;
import com.gpb.jdata.config.PersistanceTransactions;
import com.gpb.jdata.config.PersistanceTransactions.PgKey;
import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.log.SvoiSeverityEnum;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGClassReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGClassRepository;
import com.gpb.jdata.service.PGClassService;
import com.gpb.jdata.utils.PostgresCopyStreamer;
import com.gpb.jdata.utils.diff.ClassDiffContainer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGClassServiceImpl implements PGClassService {
    private static final Logger logger = LoggerFactory.getLogger(PGClassService.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGClassRepository pgClassRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final PostgresCopyStreamer postgresCopyStreamer;

    private final ClassDiffContainer diffContainer;

    private final PersistanceTransactions transactions;

    private final static String masterQuery = """
                    SELECT oid, relname, relnamespace, relkind
                    FROM pg_catalog.pg_class
                    """;
    private final static String initialCopySql = """
                    COPY jdata.pg_class_rep (
                        oid, relname, relnamespace, relkind
                    )
                    FROM STDIN WITH (FORMAT text)
                    """;
    private final static String copySql = """
                    COPY jdata.pg_class_rep_tmp (
                        oid, relname, relnamespace, relkind
                    )
                    FROM STDIN WITH (FORMAT text)
                    """;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
			"startInitSnapshot", 
			"Start PGClass init", 
			"Started PGClass init", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            logger.info("[pg_class] Starting initial snapshot (streaming COPY)...");
            svoiLogger.logConnectToSource();
            long inserted = postgresCopyStreamer.streamCopy(
                    masterQuery, this::serializeRowFromResultSet, initialCopySql);

            writeStatistics(inserted, "pg_class_rep", connection);
            logAction("INITIAL_SNAPSHOT", "pg_class", 
                    inserted + " records added", "");

            logger.info("[pg_class] Initial snapshot finished. Inserted: {}", inserted);
        } catch (SQLException | IOException e) {
            logger.error("[pg_class] Error during initial snapshot", e);
            svoiLogger.logDbConnectionError(e);
            throw (e instanceof SQLException) ? (SQLException) e : 
                    new SQLException("Initial snapshot failed", e);
        }
    }

    /**
     * Периодическая синхронизация данных
     */
    @Override
    public void synchronize() throws SQLException {
        svoiLogger.send(
			"startSync", 
			"Start PGClass sync", 
			"Started PGClass sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            long currentTransactionCount = getTransactionCount(connection);

            long lastTransactionCount = transactions.get(PgKey.PG_CLASS);

            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_class] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_class] {} Changes detected. Starting synchronization...", diff);
            
            pgClassRepository.truncateTempTable();
            postgresCopyStreamer.streamCopy(
                    masterQuery, this::serializeRowFromResultSet, copySql);

            compareSnapshots();
            transactions.put(PgKey.PG_CLASS, currentTransactionCount);
            writeStatistics(currentTransactionCount, "pg_class", connection);
        } catch (SQLException | IOException e) {
            logger.error("[pg_class] Error during synchronization", e);
            svoiLogger.logDbConnectionError(e);
            throw (e instanceof SQLException) ? (SQLException) e : 
                    new SQLException("Synchronization failed", e);
        }
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots() throws SQLException {
        List<PGClassReplication> toAdd = pgClassRepository.findNew();
        List<PGClassReplication> toUpdate = pgClassRepository.findUpdated();
        List<PGClassReplication> toDelete = pgClassRepository.findDeleted();
        List<Long> idsToDelete = toDelete.stream()
                .map(r -> r.getOid())
                .collect(Collectors.toList());

        if (!toDelete.isEmpty()) {
            logger.info("[pg_attribute_rep] Deleting {} records from the replica table", toDelete.size());
            pgClassRepository.deleteAllById(idsToDelete);
            toDelete.forEach(e -> {
                    diffContainer.addDeleted(String.format("%s.%s",
                            e.getRelnamespace(),
                            e.getRelname()));
                    diffContainer.addDeletedOids(e.getOid());
            });
            logAction("DELETE", "pg_class_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_class_rep] Adding {} records to the replica table", toAdd.size());
            pgClassRepository.saveAll(toAdd);
            toAdd.forEach(e -> diffContainer.addUpdated(e.getOid()));
            logAction("INSERT", "pg_class_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_class_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e -> {
                    diffContainer.addUpdated(e.getOid());
            });
            pgClassRepository.saveAll(toUpdate);
            logAction("UPDATE", "pg_attribute_rep", toUpdate.size() 
                    + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        try (Connection connection = databaseConfig.getConnection()) {
            writeStatistics(totalOperations, "pg_class_rep", connection);
        }
    }

    /**
     * Получение количества операций для таблицы pg_class
     */
    private long getTransactionCount(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = ? AND relname = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_class");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong("count");
            }
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    private String serializeRowFromResultSet(ResultSet rs) {
        try {
            long oid = rs.getLong("oid");
            String relname = rs.getString("relname");
            Integer relnamespace = rs.getInt("relnamespace");
            String relkind = rs.getString("relkind");

            StringBuilder sb = new StringBuilder(128);
            sb.append(escape(oid)).append('\t');
            sb.append(escape(relname)).append('\t');
            sb.append(escape(relnamespace)).append('\t');
            sb.append(escape(relkind)).append('\n');

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
     * Запись статистики
     */
    private void writeStatistics(Long namespaceOperations, String name, Connection connection) throws SQLException {
        try (Session session = postgreSessionFactory.openSession()) {
            session.beginTransaction();
            Statistics stat = new Statistics();
            String dbName = connection.getMetaData().getURL();
            dbName = dbName.substring(dbName.lastIndexOf("/") + 1);
            stat.setDb(dbName);
            stat.setSchema("pg_catalog");
            stat.setTable_name(name);
            stat.setSum(namespaceOperations);
            stat.setTimestamp(new Timestamp(System.currentTimeMillis()));
            session.persist(stat);
            session.getTransaction().commit();
        }
        logger.info("[pg_class] Statistics written for table {}", name);
    }
    /**
     * Логирование действий
     */
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
