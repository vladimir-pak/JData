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
import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.log.SvoiSeverityEnum;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGTypeReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGTypeRepository;
import com.gpb.jdata.service.PGTypeService;
import com.gpb.jdata.utils.PostgresCopyStreamer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGTypeServiceImpl implements PGTypeService {
    private static final Logger logger = LoggerFactory.getLogger(PGTypeService.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGTypeRepository pgTypeRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final PostgresCopyStreamer postgresCopyStreamer;

    private long lastTransactionCount = 0;

    private final static String masterQuery = """
                    SELECT oid, typname, typnamespace
                    FROM pg_catalog.pg_type
                    """;
    private final static String initialCopySql = """
                    COPY jdata.pg_type_rep (
                        oid, typname, typnamespace
                    )
                    FROM STDIN WITH (FORMAT text)
                    """;
    private final static String copySql = """
                    COPY jdata.pg_type_rep_tmp (
                        oid, typname, typnamespace
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
			"Start PGType init", 
			"Started PGType init", 
			SvoiSeverityEnum.ONE);

        try (Connection connection = databaseConfig.getConnection()) {
            logger.info("[pg_type] Starting initial snapshot (streaming COPY)...");
            
            long inserted = postgresCopyStreamer.streamCopy(
                    masterQuery, this::serializeRowFromResultSet, initialCopySql);

            writeStatistics(inserted, "pg_type_rep", connection);
            logAction("INITIAL_SNAPSHOT", "pg_type", 
                    inserted + " records added", "");

            logger.info("[pg_type] Initial snapshot finished. Inserted: {}", inserted);
        } catch (SQLException | IOException e) {
            logger.error("[pg_type] Error during initial snapshot", e);
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
     * Периодическая синхронизация данных
     */
    @Override
    public void synchronize() throws SQLException {
        svoiLogger.send(
			"startSync", 
			"Start PGType sync", 
			"Started PGType sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCountMain(connection);

            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_type] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_type] {} Changes detected. Starting synchronization...", diff);
            pgTypeRepository.truncateTempTable();
            postgresCopyStreamer.streamCopy(
                    masterQuery, this::serializeRowFromResultSet, copySql);

            compareSnapshots();
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_type", connection);
        } catch (SQLException | IOException e) {
            logger.error("[pg_type] Error during synchronization", e);
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
     * Сравнение снапшотов
     */
    private void compareSnapshots() throws SQLException {
        List<PGTypeReplication> toAdd = pgTypeRepository.findNew();
        List<PGTypeReplication> toUpdate = pgTypeRepository.findUpdated();
        List<PGTypeReplication> toDelete = pgTypeRepository.findDeleted();
        List<Long> idsToDelete = toDelete.stream()
                .map(r -> r.getOid())
                .collect(Collectors.toList());

        if (!toDelete.isEmpty()) {
            logger.info("[pg_type_rep] Deleting {} records from the replica table", toDelete.size());
            pgTypeRepository.deleteAllById(idsToDelete);
            logAction("DELETE", "pg_type_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_type_rep] Adding {} records to the replica table", toAdd.size());
            pgTypeRepository.saveAll(toAdd);
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_type_rep] Updating {} records in the replica table", toUpdate.size());
            pgTypeRepository.saveAll(toUpdate);
            logAction("UPDATE", "pg_type_rep", toUpdate.size() 
                    + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        try (Connection connection = databaseConfig.getConnection()) {
            writeStatistics(totalOperations, "pg_type_rep", connection);
        }
    }

    /**
     * Получение количества операций для таблицы pg_type
     */
    private long getTransactionCountMain(Connection connection) throws SQLException {
        String query = """
                SELECT n_tup_del + n_tup_ins + n_tup_upd as count
                FROM pg_catalog.pg_stat_all_tables WHERE schemaname = ? AND relname = ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_type");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong("count");
            }
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    private String serializeRowFromResultSet(ResultSet rs) {
        try {
            Long oid = rs.getLong("oid");
            String typname = rs.getString("typname");
            Long typnamespace = rs.getLong("typnamespace");

            StringBuilder sb = new StringBuilder(128);
            sb.append(escape(oid)).append('\t');
            sb.append(escape(typname)).append('\t');
            sb.append(escape(typnamespace)).append('\n');

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
        logger.info("[pg_type] Statistics written for table {}", name);
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
