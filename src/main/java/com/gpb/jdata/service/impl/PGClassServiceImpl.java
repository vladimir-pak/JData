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
import com.gpb.jdata.models.master.PGClass;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGClassReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGClassRepository;
import com.gpb.jdata.service.PGClassService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGClassServiceImpl implements PGClassService {
    private static final Logger logger = LoggerFactory.getLogger(PGClassService.class);

    private final PGClassRepository pgClassRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private long lastTransactionCount = 0;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public List<PGClass> initialSnapshot(Connection connection) throws SQLException {
        List<PGClass> data = readMasterData(connection);
        logger.info("[pg_class] Initial snapshot created...");

        filterData(data);
        replicate(data, connection);
        writeStatistics((long) data.size(), "pg_class", connection);
        logAction("INITIAL_SNAPSHOT", "pg_class", data.size() + " records added", "");
        return data;
    }

    /**
     * Периодическая синхронизация данных
     */
    @Override
    public void synchronize() {
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCount(connection);

            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_class] {} No changes detected. Skipping synchronization.", currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_class] {} Changes detected. Starting synchronization...", diff);

            List<PGClass> newData = readMasterData(connection);
            filterData(newData);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_class", connection);
        } catch (SQLException e) {
            logger.error("[pg_class] Error during synchronization", e);
        }
    }

    /**
     * Чтение данных из таблицы pg_class
     */
    private List<PGClass> readMasterData(Connection connection) throws SQLException {
        List<PGClass> data = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT oid, * FROM pg_catalog.pg_class")) {

            while (resultSet.next()) {
                data.add(new PGClass(
                        resultSet.getLong("oid"),
                        resultSet.getString("relname"),
                        resultSet.getBigDecimal("relnamespace").toBigInteger(),
                        resultSet.getString("relkind")
                ));
            }
        }
        return data;
    }

    /**
     * Фильтрация данных
     */
    private void filterData(List<PGClass> data) {
        data.removeIf(d -> d.getRelkind().equals("i")
                || d.getRelkind().equals("c")
                || d.getRelkind().equals("S"));
        data.removeIf(d -> d.getRelname().contains("_pkey")
                || d.getRelname().contains("_seq"));
    }

    /**
     * Репликация данных в таблицу репликации
     */
    @Override
    public void replicate(List<PGClass> data, Connection connection) throws SQLException {
        List<PGClassReplication> replicationData = data.stream()
                .map(d -> new PGClassReplication(
                        d.getOid(),
                        d.getRelname(),
                        d.getRelnamespace(),
                        d.getRelkind()))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgClassRepository.saveAll(replicationData);
            logger.info("[pg_class_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_class_rep", connection);
        } else {
            logger.info("[pg_class_rep] Data is empty.");
        }
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGClass> newData, Connection connection) throws SQLException {
        List<PGClassReplication> replicationData = pgClassRepository.findAll();
        Map<Long, PGClassReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGClassReplication::getOid, r -> r));

        List<PGClass> toAdd = new ArrayList<>();
        List<PGClass> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGClass masterRecord : newData) {
            Long oid = masterRecord.getOid();
            PGClassReplication replicationRecord = replicationMap.get(oid);
            if (replicationRecord == null) {
                toAdd.add(masterRecord);
            } else {
                if (!convertToReplication(masterRecord).equals(replicationRecord)) {
                    toUpdate.add(masterRecord);
                }
                toDelete.remove(oid);
            }
        }

        if (!toDelete.isEmpty()) {
            logger.info("[pg_class_rep] Deleting {} records from the replica table", toDelete.size());
            pgClassRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_class_rep", toDelete.size() + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_class_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            logAction("INSERT", "pg_class_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_class_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e ->
                    logAction("UPDATE", "pg_class_rep", " old: " +
                                    pgClassRepository.findPGClassReplicationByOid(e.getOid())
                            , " new:" + e.toString())
            );
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_class_rep", toUpdate.size() + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_class_rep", connection);
    }

    /**
     * Конвертация объекта
     */
    private PGClassReplication convertToReplication(PGClass pgClass) {
        PGClassReplication replication = new PGClassReplication();
        replication.setOid(pgClass.getOid());
        replication.setRelname(pgClass.getRelname());
        replication.setRelnamespace(pgClass.getRelnamespace());
        replication.setRelkind(pgClass.getRelkind());
        return replication;
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
