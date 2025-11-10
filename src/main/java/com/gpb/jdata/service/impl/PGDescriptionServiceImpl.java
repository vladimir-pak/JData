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
import com.gpb.jdata.models.master.PGDescription;
import com.gpb.jdata.models.master.PGDescriptionId;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGDescriptionReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGDescriptionRepository;
import com.gpb.jdata.service.PGDescriptionService;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGDescriptionServiceImpl implements PGDescriptionService {
    private static final Logger logger = LoggerFactory.getLogger(PGDescriptionService.class);

    private final PGDescriptionRepository pgDescriptionRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private long lastTransactionCount = 0;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Transactional
    @Override
    public List<PGDescription> initialSnapshot(Connection connection) throws SQLException {
        List<PGDescription> data = readMasterData(connection);
        logger.info("[pg_description] Initial snapshot created...");

        replicate(data, connection);
        writeStatistics((long) data.size(), "pg_description", connection);
        logAction("INITIAL_SNAPSHOT", "pg_description", data.size() + " records added", "");
        return data;
    }

    /**
     * Периодическая синхронизация данных
     */
    @Override
    public void synchronize() {
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCountMain(connection);

            if (currentTransactionCount == lastTransactionCount && lastTransactionCount != 0) {
                logger.info("[pg_description] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_description] {} Changes detected. Starting synchronization...", diff);

            List<PGDescription> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_description", connection);
        } catch (SQLException e) {
            logger.error("[pg_description] Error during synchronization", e);
        }
    }

    /**
     * Чтение данных из pg_description
     */
    @Override
    public List<PGDescription> readMasterData(Connection connection) throws SQLException {
        List<PGDescription> masterData = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_description")) {
            while (resultSet.next()) {
                PGDescriptionId id = new PGDescriptionId(
                        resultSet.getLong("objoid"),
                        resultSet.getInt("objsubid")
                );
                PGDescription replication = new PGDescription();
                replication.setId(id);
                replication.setDescription(resultSet.getString("description"));
                masterData.add(replication);
            }
        }
        return masterData;
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGDescription> data, Connection connection) throws SQLException {
        List<PGDescriptionReplication> replicationData = data.stream()
                .map(d -> convertToReplication(d, "adb"))
                .collect(Collectors.toList());
        pgDescriptionRepository.saveAll(replicationData);
        logger.info("[pg_description_rep] Data replicated successfully.");
        writeStatistics((long) replicationData.size(), "pg_description_rep", connection);
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGDescription> newData, Connection connection) throws SQLException {
        List<PGDescriptionReplication> replicationData = pgDescriptionRepository.findAll();
        Map<PGDescriptionId, PGDescriptionReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGDescriptionReplication::getId, r -> r));

        List<PGDescription> toAdd = new ArrayList<>();
        List<PGDescription> toUpdate = new ArrayList<>();
        List<PGDescriptionId> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGDescription masterRecord : newData) {
            PGDescriptionId id = masterRecord.getId();
            PGDescriptionReplication replicationRecord = replicationMap.get(id);
            if (replicationRecord == null) {
                toAdd.add(masterRecord);
            } else {
                if (!convertToReplication(masterRecord, "adb").equals(replicationRecord)) {
                    toUpdate.add(masterRecord);
                }
                toDelete.remove(id);
            }
        }

        if (!toDelete.isEmpty()) {
            logger.info("[pg_description_rep] Deleting {} records from the replica table", toDelete.size());
            pgDescriptionRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_description_rep", toDelete.size() + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_description_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            logAction("INSERT", "pg_description_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_description_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e ->
                    logAction("UPDATE", "pg_description_rep", " old: " +
                                    pgDescriptionRepository.findPGDescriptionReplicationById(e.getId())
                            , " new:" + e.toString())
            );
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_description_rep", toUpdate.size() + " records updated", "");
        }
        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_description_rep", connection);
    }

    /**
     * Получение количества операций для таблицы pg_description
     */
    private long getTransactionCountMain(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd as count FROM pg_catalog.pg_stat_all_tables WHERE schemaname = ? AND relname = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_description");
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
    private PGDescriptionReplication convertToReplication(PGDescription pgDescription, String db) {
        PGDescriptionId id = pgDescription.getId();
        PGDescriptionReplication replication = new PGDescriptionReplication();
        replication.setId(id);
        replication.setDescription(pgDescription.getDescription());
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
        logger.info("[pg_description] Statistics written for table {}", name);
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
