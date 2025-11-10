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
import com.gpb.jdata.models.master.PGAttrdef;
import com.gpb.jdata.models.master.PGAttrdefId;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGAttrdefReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGAttrdefRepository;
import com.gpb.jdata.service.PGAttrdefService;

import lombok.RequiredArgsConstructor;

/**
pg_attrdef service provides creation of master data from pg_catalog.pg_attrdef
and inserting, deleting or updating it in the replica tables.
 */
@Service
@RequiredArgsConstructor
public class PGAttrdefServiceImpl implements PGAttrdefService {

    private static final Logger logger = LoggerFactory.getLogger(PGAttrdefService.class);

    private final ActionRepository actionRepository;
    private final PGAttrdefRepository pgAttrdefRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;
    
    private long lastTransactionCount = 0;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public List<PGAttrdef> initialSnapshot(Connection connection) throws SQLException {
        List<PGAttrdef> data = readMasterData(connection);
        logger.info("[pg_attrdef] Initial snapshot created...");
        replicate(data, connection);
        writeStatistics((long) data.size(), "pg_attrdef", connection);
        logAction("INITIAL_SNAPSHOT", "pg_attrdef", data.size() + " records added", "");
        return data;
    }

    /**
     * Синхронизация данных
     */
    @Override
    public void synchronize() {
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCount(connection);

            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_attrdef] No changes detected. Skipping synchronization.");
                return;
            }
            
            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_attrdef] {} Changes detected. Starting synchronization...", diff);

            List<PGAttrdef> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_attrdef", connection);

        } catch (SQLException e) {
            logger.error("[pg_attrdef] Error during synchronization", e);
        }
    }

    /**
     * Чтение данных из таблицы pg_attrdef
     */
    // alrelid + adnum | adbin колонка доп
    private List<PGAttrdef> readMasterData(Connection connection) throws SQLException {
        List<PGAttrdef> data = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_attrdef")) {
            while (resultSet.next()) {
                data.add(new PGAttrdef(
                        resultSet.getLong("adrelid"),
                        resultSet.getLong("adnum"),
                        resultSet.getString("adbin")
                ));
            }
        }
        return data;
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGAttrdef> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);

        List<PGAttrdefReplication> replicationData = data.stream()
                .map(d -> new PGAttrdefReplication(new PGAttrdefId(d.getAdrelid(), d.getAdnum()), d.getAdbin(), "pg_attrdef_rep"))
                .collect(Collectors.toList());
        pgAttrdefRepository.saveAll(replicationData);
        logger.info("[pg_attrdef_rep] Data replicated successfully.");
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGAttrdef> newData, Connection connection) throws SQLException {
        List<PGAttrdefReplication> replicationData = pgAttrdefRepository.findAll();
        Map<PGAttrdefId, PGAttrdefReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGAttrdefReplication::getId, r -> r));

        List<PGAttrdef> toAdd = new ArrayList<>();
        List<PGAttrdef> toUpdate = new ArrayList<>();
        List<PGAttrdefId> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGAttrdef masterRecord : newData) {
            PGAttrdefId oid = new PGAttrdefId(masterRecord.getAdrelid(), masterRecord.getAdnum());
            PGAttrdefReplication replicationRecord = replicationMap.get(oid);
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
            logger.info("[pg_attrdef_rep] Deleting {} records from the replica table", toDelete.size());
            pgAttrdefRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_attrdef_rep", toDelete.size() + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_attrdef_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            logAction("INSERT", "pg_attrdef_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_attrdef_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e ->
                    logAction("UPDATE", "pg_attrdef_rep", " old: " +
                                    pgAttrdefRepository.findById(new PGAttrdefId(e.getAdrelid(), e.getAdnum()))
                            , " new:" + e.toString())
            );
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_attrdef_rep", toUpdate.size() + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_attrdef_rep", connection);
    }

    /**
     * Конвертация объекта
     */
    private PGAttrdefReplication convertToReplication(PGAttrdef pgAttrdef) {
        return new PGAttrdefReplication(new PGAttrdefId(pgAttrdef.getAdrelid(), pgAttrdef.getAdnum()), pgAttrdef.getAdbin(), "pg_attrdef_rep");
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
