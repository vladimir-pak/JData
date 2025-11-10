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
import com.gpb.jdata.models.master.PGType;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGTypeReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGTypeRepository;
import com.gpb.jdata.service.PGTypeService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGTypeServiceImpl implements PGTypeService {
    private static final Logger logger = LoggerFactory.getLogger(PGTypeService.class);

    private final PGTypeRepository pgTypeRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private long lastTransactionCount = 0;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public List<PGType> initialSnapshot(Connection connection) throws SQLException {
        List<PGType> data = readMasterData(connection);
        logger.info("[pg_type] Initial snapshot created...");
        replicate(data, connection);
        writeStatistics((long) data.size(), "pg_type", connection);
        logAction("INITIAL_SNAPSHOT", "pg_type", data.size() 
                + " records added", "");
        return data;
    }

    /**
     * Периодическая синхронизация данных
     */
    @Override
    public void synchronize() {
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCountMain(connection);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_type] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }
            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_type] {} Changes detected. Starting synchronization...", diff);
            List<PGType> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_type", connection);
        } catch (SQLException e) {
            logger.error("[pg_type] Error during synchronization", e);
        }
    }

    /**
     * Чтение данных из таблицы pg_type
     */
    @Override
    public List<PGType> readMasterData(Connection connection) throws SQLException {
        List<PGType> masterData = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT oid, * FROM pg_catalog.pg_type")) {
            while (resultSet.next()) {
                PGType replication = new PGType();
                replication.setOid(resultSet.getLong("oid"));
                replication.setTypnamespace(resultSet.getLong("typnamespace"));
                replication.setTypname(resultSet.getString("typname"));
                masterData.add(replication);
            }
        }
        return masterData;
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGType> data, Connection connection) throws SQLException {
        List<PGTypeReplication> replicationData = data.stream()
                .map(d -> convertToReplication(d, "adb"))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgTypeRepository.saveAll(replicationData);
            logger.info("[pg_type_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_type_rep", connection);
        } else {
            logger.info("[pg_type_rep] Data is empty.");
        }
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGType> newData, Connection connection) throws SQLException {
        List<PGTypeReplication> replicationData = pgTypeRepository.findAll();
        Map<Long, PGTypeReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGTypeReplication::getOid, r -> r));

        List<PGType> toAdd = new ArrayList<>();
        List<PGType> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGType masterRecord : newData) {
            Long oid = masterRecord.getOid();
            PGTypeReplication replicationRecord = replicationMap.get(oid);
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
            logger.info("[pg_type_rep] Deleting {} records from the replica table", toDelete.size());
            pgTypeRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_type_rep", toDelete.size() + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_type_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            logAction("INSERT", "pg_type_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_type_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e ->
                    logAction("UPDATE", "pg_type_rep", " old: " +
                                    pgTypeRepository.findPGTypeReplicationByOid(e.getOid())
                            , " new:" + e.toString())
            );
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_type_rep", toUpdate.size() + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_type_rep", connection);
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

    /**
     * Конвертация объекта
     */
    private PGTypeReplication convertToReplication(PGType pgType, String db) {
        PGTypeReplication replication = new PGTypeReplication();
        replication.setOid(pgType.getOid());
        replication.setTypnamespace(pgType.getTypnamespace());
        replication.setTypname(pgType.getTypname());
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
