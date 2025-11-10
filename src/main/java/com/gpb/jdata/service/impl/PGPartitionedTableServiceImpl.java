package com.gpb.jdata.service.impl;

import java.sql.Connection;
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
import com.gpb.jdata.models.master.PGPartitionedTable;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGPartitionedTableReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGPartitionedTableRepository;
import com.gpb.jdata.service.PGPartitionedTableService;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGPartitionedTableServiceImpl implements PGPartitionedTableService {
    private static final Logger logger = LoggerFactory.getLogger(PGPartitionedTableService.class);

    private final PGPartitionedTableRepository pgPartitionedTableRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */

    @Transactional
    @Override
    public List<PGPartitionedTable> initialSnapshot(Connection connection) throws SQLException {
        List<PGPartitionedTable> data = readMasterData(connection);
        logger.info("[pg_partitioned_table] Initial snapshot created...");
        replicate(data, connection);
        writeStatistics((long) data.size(), "pg_partitioned_table", connection);
        logAction("INITIAL_SNAPSHOT", "pg_partitioned_table", 
                data.size() + " records added", "");
        return data;
    }
    
    /**
     * Синхронизация данных
     */
    @Transactional
    @Override
    public void synchronize() {
        try (Connection connection = databaseConfig.getConnection()) {
        //    long currentTransactionCount = getTransactionCountMain(connection);
        //    if (currentTransactionCount == lastTransactionCount) {
        //        logger.info("[pg_partitioned_table] {} No changes detected. Skipping synchronization.", currentTransactionCount);
        //        return;
        //    }

        //    long diff = currentTransactionCount - lastTransactionCount;
        //    logger.info("[pg_partitioned_table] {} Changes detected. Starting synchronization...", diff);
            List<PGPartitionedTable> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
        //    lastTransactionCount = currentTransactionCount;
        //    writeStatistics(currentTransactionCount, "pg_partitioned_table", connection);
        } catch (SQLException e) {
            logger.error("[pg_partitioned_table] Error during synchronization", e);
        }
    }

    /**
     * Чтение данных из pg_partitioned_table
     */
    @Override
    public List<PGPartitionedTable> readMasterData(Connection connection) throws SQLException {
        List<PGPartitionedTable> masterData = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_partitioned_table")) {
            while (resultSet.next()) {
                masterData.add(new PGPartitionedTable(
                        resultSet.getLong("partrelid"),
                        resultSet.getShort("partnatts"),
                        resultSet.getString("partstrat"),
                        parseArray(resultSet.getString("partattrs"))
                ));
            }
        }
        return masterData;
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGPartitionedTable> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);
        String finalDb = db;
        List<PGPartitionedTableReplication> replicationData = data.stream()
                .map(d -> convertToReplication(d, finalDb))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgPartitionedTableRepository.saveAll(replicationData);
            logger.info("[pg_partitioned_table_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_partitioned_table_rep", connection);
        } else {
            logger.info("[pg_partitioned_table_rep] Data is empty.");
        }
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGPartitionedTable> newData, Connection connection) throws SQLException {
        List<PGPartitionedTableReplication> replicationData = pgPartitionedTableRepository.findAll();
        Map<Long, PGPartitionedTableReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGPartitionedTableReplication::getPartrelid, r -> r));

        List<PGPartitionedTable> toAdd = new ArrayList<>();
        List<PGPartitionedTable> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGPartitionedTable masterRecord : newData) {
            long partrelid = masterRecord.getPartrelid();
            PGPartitionedTableReplication replicationRecord = replicationMap.get(partrelid);
            if (replicationRecord == null) {
                toAdd.add(masterRecord);
            } else {
                if (!convertToReplication(masterRecord, replicationRecord.getDb()).equals(replicationRecord)) {
                    toUpdate.add(masterRecord);
                }
                toDelete.remove(partrelid);
            }
        }

        if (!toDelete.isEmpty()) {
            logger.info("[pg_partitioned_table_rep] Deleting {} records from the replica table", toDelete.size());
            pgPartitionedTableRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_partitioned_table_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_partitioned_table_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            logAction("INSERT", "pg_partitioned_table_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_partitioned_table_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e ->
                    logAction("UPDATE", "pg_partitioned_table_rep", " old: " +
                                    pgPartitionedTableRepository.findById(e.getPartrelid().longValue())
                            , " new:" + e.toString())
            );
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_partitioned_table_rep", toUpdate.size() 
                    + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_partitioned_table_rep", connection);
    }

    /**
     * Конвертация объекта
     */
    private PGPartitionedTableReplication convertToReplication(PGPartitionedTable pgPartitionedTable, String db) {
        PGPartitionedTableReplication replication = new PGPartitionedTableReplication();
        replication.setPartrelid(pgPartitionedTable.getPartrelid());
        replication.setPartnatts(pgPartitionedTable.getPartnatts());
        replication.setPartstrat(pgPartitionedTable.getPartstrat());
        replication.setPartattrs(pgPartitionedTable.getPartattrs());
        replication.setDb(db);
        return replication;
    }

    /**
     * Парсинг массива строк в список
     */
    private List<Integer> parseArray(String arrayString) {
        if (arrayString == null || arrayString.isEmpty()) {
            return new ArrayList<>();
        }
        String[] elements = arrayString.replaceAll("\\{", "")
                .replaceAll("\\}", "")
                .split(",");
        List<Integer> result = new ArrayList<>();
        for (String element : elements) {
            if (!element.isEmpty()) {
                result.add(Integer.parseInt(element));
            }
        }
        return result;
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
        logger.info("[pg_partitioned_table] Statistics written for table {}", name);
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
