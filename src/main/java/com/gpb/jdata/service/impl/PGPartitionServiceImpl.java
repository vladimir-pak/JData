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
import com.gpb.jdata.models.master.PGPartition;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGPartitionReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGPartitionRepository;
import com.gpb.jdata.service.PGPartitionService;
import com.gpb.jdata.utils.diff.ClassDiffContainer;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGPartitionServiceImpl implements PGPartitionService {
    private static final Logger logger = LoggerFactory.getLogger(PGPartitionServiceImpl.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGPartitionRepository pgPartitionRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final ClassDiffContainer diffContainer;

    private long lastTransactionCount = 0;
   
    /**
     * Создание начального снапшота и запись данных в репликацию
     */
    @Transactional
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
			"startInitSnapshot", 
			"Start PGPartition init", 
			"Started PGPartition init", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            List<PGPartition> data = readMasterData(connection);
            logger.info("[pg_partition] Initial snapshot created...");
            replicate(data, connection);
            writeStatistics((long) data.size(), "pg_partition", connection);
            logAction("INITIAL_SNAPSHOT", "pg_partition", data.size() 
                    + " records added", "");
        } catch (SQLException e) {
            logger.error("[pg_partition] Error during initialization", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> initialSnapshotAsync() throws SQLException {
        initialSnapshot();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Периодическая синхронизация
     */
    @Override
    @Transactional
    public void synchronize() {
        svoiLogger.send(
			"startSync", 
			"Start PGPartition sync", 
			"Started PGPartition sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCount(connection);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_partition] No changes detected. Skipping synchronization.");
                return;
            }
            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_partition] {} changes detected. Synchronizing...", diff);
            List<PGPartition> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_partition_rep", connection);
        } catch (SQLException e) {
            logger.error("[pg_partition] Error during synchronization", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> synchronizeAsync() {
        synchronize();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Чтение всех записей из pg_partition
     */
    private List<PGPartition> readMasterData(Connection connection) throws SQLException {
        List<PGPartition> data = new ArrayList<>();
        String sql = "SELECT oid, parrelid, parnatts, parkind, paratts FROM pg_catalog.pg_partition";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                PGPartition p = new PGPartition(
                        rs.getLong("oid"),
                        rs.getLong("parrelid"),
                        rs.getInt("parnatts"),
                        rs.getString("parkind"),
                        (Integer[]) rs.getArray("paratts").getArray()
                );
                data.add(p);
            }
        }
        return data;
    }

    /**
     * Реплицирует список сущностей
     */
    @Override
    public void replicate(List<PGPartition> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);
        String finalDb = db;
        List<PGPartitionReplication> replicated = data.stream()
                .map(d -> new PGPartitionReplication(
                        d.getOid(), d.getParrelid(), d.getParnatts(),
                        d.getParkind(), d.getParatts(), finalDb))
                .collect(Collectors.toList());

        if (replicated != null && !replicated.isEmpty()) {
            pgPartitionRepository.saveAll(replicated);
            logger.info("[pg_partition_rep] Data replicated successfully.");
            writeStatistics((long) replicated.size(), "pg_partition_rep", connection);
        } else {
            logger.info("[pg_partition_rep] Data is empty.");
        }
    }

    /**
     * Сравнение снапшотов и обновление репликации
     */
    private void compareSnapshots(List<PGPartition> newData, Connection connection) throws SQLException {
        List<PGPartitionReplication> repData = pgPartitionRepository.findAll();
        Map<Long, PGPartitionReplication> repMap = repData.stream()
                .collect(Collectors.toMap(PGPartitionReplication::getOid, r -> r));

        List<PGPartition> toAdd = new ArrayList<>();
        List<PGPartition> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(repMap.keySet());
        
        for (PGPartition master : newData) {
            PGPartitionReplication rep = repMap.get(master.getOid());
            if (rep == null) {
                toAdd.add(master);
            } else {
                if (!convertToReplication(master, rep.getDb()).equals(rep)) {
                    toUpdate.add(master);
                }
                toDelete.remove(master.getOid());
            }
        }

        if (!toDelete.isEmpty()) {
            pgPartitionRepository.deleteAllById(toDelete);
            toDelete.forEach(e -> {
                    if (!diffContainer.containsInDeletedOids(e)) {
                        diffContainer.addUpdated(e);
                    }
            });
            logAction("DELETE", "pg_partition_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            replicate(toAdd, connection);
            toAdd.forEach(e -> diffContainer.addUpdated(e.getOid()));
            logAction("INSERT", "pg_partition_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            replicate(toUpdate, connection);
            toUpdate.forEach(e -> diffContainer.addUpdated(e.getOid()));
            logAction("UPDATE", "pg_partition_rep", toUpdate.size() 
                    + " records updated", "");
        }
    }

    /**
     * Конвертация master -> replication сущность
     */
    private PGPartitionReplication convertToReplication(PGPartition master, String db) {
        return new PGPartitionReplication(
                master.getOid(), master.getParrelid(), master.getParnatts(),
                master.getParkind(), master.getParatts(), db);
    }

    /**
     * Получение числа операций в таблице
     */
    private long getTransactionCount(Connection connection) throws SQLException {
        String sql = "SELECT n_tup_ins + n_tup_upd + n_tup_del AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = ? AND relname = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, "pg_catalog");
            ps.setString(2, "pg_partition");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return rs.getLong("count");
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    /**
     * Сохранение статистики
     */
    private void writeStatistics(Long operations, String tableName, Connection connection) throws SQLException {
        try (Session session = postgreSessionFactory.openSession()) {
            session.beginTransaction();
            Statistics stat = new Statistics();
            String dbName = connection.getMetaData().getURL();
            dbName = dbName.substring(dbName.lastIndexOf("/") + 1);
            stat.setDb(dbName);
            stat.setSchema("pg_catalog");
            stat.setTable_name(tableName);
            stat.setSum(operations);
            stat.setTimestamp(new Timestamp(System.currentTimeMillis()));
            session.persist(stat);
            session.getTransaction().commit();
        }
        logger.info("[pg_partition] Statistics written for table {}", tableName);
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
