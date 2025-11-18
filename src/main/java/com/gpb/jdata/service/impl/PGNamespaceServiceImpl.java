package com.gpb.jdata.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.log.SvoiSeverityEnum;
import com.gpb.jdata.models.master.PGNamespace;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGNamespaceReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGNamespaceRepository;
import com.gpb.jdata.service.PGNamespaceService;
import com.gpb.jdata.utils.diff.NamespaceDiffContainer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGNamespaceServiceImpl implements PGNamespaceService {
    private static final Logger logger = LoggerFactory.getLogger(PGNamespaceService.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGNamespaceRepository pgNamespaceRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final NamespaceDiffContainer diffContainer;

    private long lastTransactionCount = 0;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
			"startInitSnapshot", 
			"Start PGNamespace init", 
			"Started PGNamespace init", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            List<PGNamespace> data = readMasterData(connection);
            logger.info("[pg_namespace] Initial snapshot created...");
            replicate(data, connection);
            writeStatistics((long) data.size(), "pg_namespace", connection);
            data.forEach(e -> diffContainer.addUpdated(e.getOid()));
            logAction("INITIAL_SNAPSHOT", "pg_namespace", 
                    data.size() + " records added", "");
        } catch (SQLException e) {
            logger.error("[pg_namespace] Error during initialization", e);
            svoiLogger.logDbConnectionError(e);
        }
    }

    /**
     * Периодическая синхронизация данных
     */
    @Override
    public void synchronize() {
        svoiLogger.send(
			"startSync", 
			"Start PGNamespace sync", 
			"Started PGNamespace sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            long currentTransactionCount = getTransactionCountMain(connection);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_namespace] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }
            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_namespace] {} Changes detected. Starting synchronization...", diff);
            List<PGNamespace> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_namespace", connection);
        } catch (SQLException e) {
            logger.error("[pg_namespace] Error during synchronization", e);
            svoiLogger.logDbConnectionError(e);
        }
    }

    /**
     * Чтение данных из pg_namespace
     */
    @Override
    public List<PGNamespace> readMasterData(Connection connection) throws SQLException {
        List<PGNamespace> masterData = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT oid, * FROM pg_catalog.pg_namespace")) {
            while (resultSet.next()) {
                PGNamespace replication = new PGNamespace();
                replication.setOid(resultSet.getLong("oid"));
                replication.setNspname(resultSet.getString("nspname"));
                replication.setNspowner(resultSet.getLong("nspowner"));
                masterData.add(replication);
            }
        }
        return masterData;
    }

    /**
     * Репликация данных в репликации
     */
    private void replicate(List<PGNamespace> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);
        String finalDb = db;
        List<PGNamespaceReplication> replicationData = data.stream()
                .map(d -> convertToReplication(d, finalDb))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgNamespaceRepository.saveAll(replicationData);
            logger.info("[pg_namespace_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_namespace_rep", connection);
        } else {
            logger.info("[pg_namespace_rep] Data is empty.");
        }
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGNamespace> newData, Connection connection) throws SQLException {
        List<PGNamespaceReplication> replicationData = pgNamespaceRepository.findAll();
        Map<Long, PGNamespaceReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGNamespaceReplication::getOid, r -> r));

        List<PGNamespace> toAdd = new ArrayList<>();
        List<PGNamespace> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGNamespace masterRecord : newData) {
            Long oid = masterRecord.getOid();
            PGNamespaceReplication replicationRecord = replicationMap.get(oid);
            if (replicationRecord == null) {
                toAdd.add(masterRecord);
            } else {
                if (!convertToReplication(masterRecord, "adb").equals(replicationRecord)) {
                    toUpdate.add(masterRecord);
                }
                toDelete.remove(oid);
            }
        }

        if (!toDelete.isEmpty()) {
            logger.info("[pg_namespace_rep] Deleting {} records from the replica table", toDelete.size());
            pgNamespaceRepository.deleteAllById(toDelete);
            toDelete.forEach(e -> {
                    diffContainer.addDeleted(replicationMap.get(e).getNspname());
                    diffContainer.addDeletedOids(e);
            });
            logAction("DELETE", "pg_namespace_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_namespace_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            toAdd.forEach(e -> diffContainer.addUpdated(e.getOid()));
            logAction("INSERT", "pg_namespace_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_namespace_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e -> {
                    logAction("UPDATE", "pg_namespace_rep", " old: " +
                                    pgNamespaceRepository.findPGNamespaceReplicationByOid(e.getOid())
                            , " new:" + e.toString());
                    diffContainer.addUpdated(e.getOid());
            });
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_namespace_rep", toUpdate.size() 
                    + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_namespace_rep", connection);
    }
    /**
     * Получение количества операций для таблицы pg_namespace
     */
    private long getTransactionCountMain(Connection connection) throws SQLException {
        String query = """
                SELECT n_tup_del + n_tup_ins + n_tup_upd as count
                FROM pg_catalog.pg_stat_all_tables WHERE schemaname = ? AND relname = ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_namespace");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong("count");
            }
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    /**
     * Конвертация объекта
     */
    private PGNamespaceReplication convertToReplication(PGNamespace pgNamespace, String db) {
        PGNamespaceReplication replication = new PGNamespaceReplication();
        replication.setOid(pgNamespace.getOid());
        replication.setNspname(pgNamespace.getNspname());
        replication.setNspowner(pgNamespace.getNspowner());
        replication.setDb(db);
        return replication;
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
        logger.info("[pg_namespace] Statistics written for table {}", name);
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
