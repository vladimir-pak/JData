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
import java.util.Optional;
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
import com.gpb.jdata.models.master.PGViews;
import com.gpb.jdata.models.master.PGViewsId;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGViewsReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGClassRepository;
import com.gpb.jdata.repository.PGViewsRepository;
import com.gpb.jdata.service.PGViewsService;
import com.gpb.jdata.utils.diff.ClassDiffContainer;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGViewsServiceImpl implements PGViewsService {
    private static final Logger logger = LoggerFactory.getLogger(PGViewsService.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGViewsRepository pgViewsRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final PGClassRepository classRepository;

    private final ClassDiffContainer diffContainer;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     */
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
			"startInitSnapshot", 
			"Start PGViews init", 
			"Started PGViews init", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            List<PGViews> data = readMasterData(connection);
            logger.info("[pg_views] Initial snapshot created...");
            replicate(data, connection);
            writeStatistics((long) data.size(), "pg_views", connection);
            logAction("INITIAL_SNAPSHOT", "pg_views", data.size() 
                    + " records added", "");
        } catch (SQLException e) {
            logger.error("[pg_views] Error during initialization", e);
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
			"Start PGViews sync", 
			"Started PGViews sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
        //    long currentTransactionCount = getTransactionCountMain(connection);

        //    if (currentTransactionCount == lastTransactionCount) {
        //        logger.info("[pg_views] {} No changes detected. Skipping synchronization.", currentTransactionCount);
        //        return;
        //    }
        //    long diff = currentTransactionCount - lastTransactionCount;
        //    logger.info("[pg_views] {} Changes detected. Starting synchronization...", diff);
            List<PGViews> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
        //    lastTransactionCount = currentTransactionCount;

        //    writeStatistics(currentTransactionCount, "pg_views", connection);
        } catch (SQLException e) {
            logger.error("[pg_views] Error during synchronization", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> synchronizeAsync() {
        synchronize();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Чтение данных из таблицы pg_views
     */
    @Override
    public List<PGViews> readMasterData(Connection connection) throws SQLException {
        List<PGViews> masterData = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_views")) {
            while (resultSet.next()) {
                PGViewsId id = new PGViewsId();
                id.setSchemaname(resultSet.getString("schemaname"));
                id.setViewname(resultSet.getString("viewname"));
                PGViews replication = new PGViews();
                replication.setId(id);
                replication.setDefinition(resultSet.getString("definition"));
                masterData.add(replication);
            }
        }
        return masterData;
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGViews> data, Connection connection) throws SQLException {
        List<PGViewsReplication> replicationData = data.stream()
                .map(d -> convertToReplication(d, "adb"))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgViewsRepository.saveAll(replicationData);
            logger.info("[pg_views_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_views_rep", connection);
        } else {
            logger.info("[pg_views_rep] Data is empty.");
        }
    }
    
    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGViews> newData, Connection connection) throws SQLException {
        List<PGViewsReplication> replicationData = pgViewsRepository.findAll();
        Map<PGViewsId, PGViewsReplication> replicationMap = replicationData.stream()
                .collect(Collectors.toMap(PGViewsReplication::getId, r -> r));

        List<PGViews> toAdd = new ArrayList<>();
        List<PGViews> toUpdate = new ArrayList<>();
        List<PGViewsId> toDelete = new ArrayList<>(replicationMap.keySet());

        for (PGViews masterRecord : newData) {
            PGViewsId id = masterRecord.getId();
            PGViewsReplication replicationRecord = replicationMap.get(id);
            if (replicationRecord == null) {
                toAdd.add(masterRecord);
            } else {
                if (!convertToReplication(masterRecord, replicationRecord.getDb()).equals(replicationRecord)) {
                    toUpdate.add(masterRecord);
                }
                toDelete.remove(id);
            }
        }

        if (!toDelete.isEmpty()) {
            logger.info("[pg_views_rep] Deleting {} records from the replica table", toDelete.size());
            pgViewsRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_views_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_views_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            toAdd.forEach(e -> {
                    String fqn = String.format("%s.%s",
                            e.getId().getSchemaname(), e.getId().getViewname());
                    Optional<Long> oid = classRepository.findOidByFqn(fqn);
                    if (oid.isPresent()) {
                        diffContainer.addUpdated(oid.get());
                    }
            });
            logAction("INSERT", "pg_views_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
        //    logger.info("[pg_views_rep] Updating {} records in the replica table", toUpdate.size());
        //    toUpdate.forEach(e ->
        //            logAction("UPDATE", "pg_views_rep", " old: " +
        //                            pgViewsRepository.findById(e.getId())
        //                    , " new:" + e.toString())

        //    );
            replicate(toUpdate, connection);
            toUpdate.forEach(e -> {
                    String fqn = String.format("%s.%s", 
                            e.getId().getSchemaname(), e.getId().getViewname());
                    Optional<Long> oid = classRepository.findOidByFqn(fqn);
                    if (oid.isPresent()) {
                        diffContainer.addUpdated(oid.get());
                    }
            });
            logAction("UPDATE", "pg_views_rep", toUpdate.size() 
                    + " records updated", "");
        }
        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_views_rep", connection);
    }
    
    /**
     * Конвертация объекта
     */
    private PGViewsReplication convertToReplication(PGViews pgViews, String db) {
        PGViewsId id = pgViews.getId();
        PGViewsReplication replication = new PGViewsReplication();
        replication.setId(id);
        replication.setDefinition(pgViews.getDefinition());
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
        logger.info("[pg_views] Statistics written for table {}", name);
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
