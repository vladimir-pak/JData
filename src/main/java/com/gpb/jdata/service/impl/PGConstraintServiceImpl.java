package com.gpb.jdata.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import com.gpb.jdata.models.master.PGConstraint;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGConstraintReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGConstraintRepository;
import com.gpb.jdata.service.PGConstraintService;
import com.gpb.jdata.utils.diff.ClassDiffContainer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGConstraintServiceImpl implements PGConstraintService {
    private static final Logger logger = LoggerFactory.getLogger(PGConstraintService.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGConstraintRepository pgConstraintRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final ClassDiffContainer diffContainer;

    private final PersistanceTransactions transactions;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
			"startInitSnapshot", 
			"Start PGConstraint init", 
			"Started PGConstraint init", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            List<PGConstraint> data = readMasterData(connection);
            logger.info("[pg_constraint] Initial snapshot created...");

            replicate(data, connection);
            writeStatistics((long) data.size(), "pg_constraint", connection);
            logAction("INITIAL_SNAPSHOT", "pg_constraint", data.size() + " records added", "");
        } catch (SQLException e) {
            logger.error("[pg_constraint] Error during initialization", e);
            svoiLogger.logDbConnectionError(e);
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
    public void synchronize() {
        svoiLogger.send(
			"startSync", 
			"Start PGConstraint sync", 
			"Started PGConstraint sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            long currentTransactionCount = getTransactionCountMain(connection);

            long lastTransactionCount = transactions.get(PgKey.PG_CONSTRAINT);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_constraint] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_constraint] {} Changes detected. Starting synchronization...", diff);

            List<PGConstraint> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            transactions.put(PgKey.PG_CONSTRAINT, currentTransactionCount);
            writeStatistics(currentTransactionCount, "pg_constraint", connection);
        } catch (SQLException e) {
            logger.error("[pg_constraint] Error during synchronization", e);
            svoiLogger.logDbConnectionError(e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> synchronizeAsync() {
        synchronize();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Чтение данных из таблицы pg_constraint
     */
    @Override
    public List<PGConstraint> readMasterData(Connection connection) throws SQLException {
        List<PGConstraint> masterData = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_constraint")) {

            while (resultSet.next()) {
                masterData.add(new PGConstraint(
                        Long.parseLong(String.valueOf(
                           resultSet.getLong("conrelid")
                           + resultSet.getLong("contypid")
                           + resultSet.getLong("conindid")
                           + resultSet.getLong("connamespace")
                        )),
                        resultSet.getString("conname"),
                        resultSet.getLong("connamespace"),
                        resultSet.getString("contype"),
                        resultSet.getLong("conrelid"),
                        resultSet.getLong("confrelid"),
                        resultSet.getLong("contypid"),
                        parseArray(resultSet.getString("conkey")),
                        parseArray(resultSet.getString("confkey"))
                ));
            }
        }
        return masterData;
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGConstraint> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);
        String finalDb = db;
        List<PGConstraintReplication> replicationData = data.stream()
                .map(d -> convertToReplication(d, finalDb))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgConstraintRepository.saveAll(replicationData);
            logger.info("[pg_constraint_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_constraint_rep", connection);
        } else {
            logger.info("[pg_constraint_rep] Data is empty.");
        }
    }

    /**
     * * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGConstraint> newData, Connection connection) throws SQLException {
        List<PGConstraintReplication> replicationData = pgConstraintRepository.findAll();
        Map<Long, PGConstraintReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGConstraintReplication::getOid, r -> r));

        List<PGConstraint> toAdd = new ArrayList<>();
        List<PGConstraint> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGConstraint masterRecord : newData) {
            Long oid = masterRecord.getOid();
            PGConstraintReplication replicationRecord = replicationMap.get(oid);
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
            logger.info("[pg_constraint_rep] Deleting {} records from the replica table", toDelete.size());
            pgConstraintRepository.deleteAllById(toDelete);
            toDelete.forEach(e -> {
                    if (!diffContainer.containsInDeletedOids(e)) {
                        diffContainer.addUpdated(replicationMap.get(e).getConrelid());
                    }
            });
            logAction("DELETE", "pg_constraint_rep", toDelete.size() + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_constraint_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            toAdd.forEach(e -> diffContainer.addUpdated(e.getConrelid()));
            logAction("INSERT", "pg_constraint_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_constraint_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e -> {
                    logAction("UPDATE", "pg_constraint_rep", " old: " +
                                    pgConstraintRepository.findById(e.getOid().longValue())
                            , " new:" + e.toString());
                    diffContainer.addUpdated(e.getConrelid());
            });
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_constraint_rep", toUpdate.size() + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_constraint_rep", connection);
    }

    /**
     * Конвертация объекта
     */
    private PGConstraintReplication convertToReplication(PGConstraint pgConstraint, String db) {
        PGConstraintReplication replication = new PGConstraintReplication();
        replication.setOid(pgConstraint.getOid());
        replication.setConname(pgConstraint.getConname());
        replication.setConnamespace(pgConstraint.getConnamespace());
        replication.setContype(pgConstraint.getContype());
        replication.setConrelid(pgConstraint.getConrelid());
        replication.setConfrelid(pgConstraint.getConfrelid());
        replication.setContypid(pgConstraint.getContypid());
        replication.setConkey(pgConstraint.getConkey());
        replication.setConfkey(pgConstraint.getConfkey());
        replication.setDb(db);
        return replication;
    }

    /**
     * Получение количества операций для таблицы pg_constraint
     */
    private long getTransactionCountMain(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = ? AND relname = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_constraint");
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
        logger.info("[pg_constraint] Statistics written for table {}", name);
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
    
    /**
     * Парсинг массива PostgreSQL в список целых чисел
     */
    private List<Integer> parseArray(String arrayString) {
        if (arrayString == null || arrayString.isEmpty()) {
            return new ArrayList<>();
        }
        String[] elements = arrayString.replaceAll("[\\{\\}]", "").split(",");
        return Arrays.stream(elements)
                .filter(s -> !s.isEmpty())
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }
}
