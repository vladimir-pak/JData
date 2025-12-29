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
import com.gpb.jdata.models.master.PGPartitionRule;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGPartitionRuleReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGPartitionRuleRepository;
import com.gpb.jdata.service.PGPartitionRuleService;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PGPartitionRuleServiceImpl implements PGPartitionRuleService {
    private static final Logger logger = LoggerFactory.getLogger(PGPartitionRuleServiceImpl.class);
    private final SvoiCustomLogger svoiLogger;
    
    private final PGPartitionRuleRepository pgPartitionRuleRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final PersistanceTransactions transactions;

    @Override
    @Transactional
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
			"startInitSnapshot", 
			"Start PGPartitionRule init", 
			"Started PGPartitionRule init", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            svoiLogger.logConnectToSource();
            List<PGPartitionRule> data = readMasterData(connection);
            logger.info("[pg_partition_rule] Initial snapshot created...");
            replicate(data, connection);
            writeStatistics((long) data.size(), "pg_partition_rule", connection);
            logAction("INITIAL_SNAPSHOT", "pg_partition_rule", data.size() 
                    + " records added", "");
        } catch (SQLException e) {
            logger.error("[pg_partition_rule] Error during initialization", e);
            svoiLogger.logDbConnectionError(e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> initialSnapshotAsync() throws SQLException {
        initialSnapshot();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Transactional
    public void synchronize() {
        svoiLogger.send(
			"startSync", 
			"Start PGPartitionRule sync", 
			"Started PGPartitionRule sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCount(connection);
            long lastTransactionCount = transactions.get(PgKey.PG_PARYIYION_RULE);
            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_partition_rule] No changes detected.");
                return;
            }
            List<PGPartitionRule> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_partition_rule", connection);
        } catch (SQLException e) {
            logger.error("[pg_partition_rule] Error during synchronization", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> synchronizeAsync() {
        synchronize();
        return CompletableFuture.completedFuture(null);
    }

    private List<PGPartitionRule> readMasterData(Connection connection) throws SQLException {
        List<PGPartitionRule> data = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                    "SELECT oid, parchildrelid, paroid FROM pg_catalog.pg_partition_rule")) {
            while (rs.next()) {
                data.add(new PGPartitionRule(
                        rs.getLong("oid"),
                        rs.getLong("parchildrelid"),
                        rs.getLong("paroid")
                ));
            }
        }
        return data;
    }

    @Override
    public void replicate(List<PGPartitionRule> data, Connection connection) throws SQLException {
        String db = connection.getMetaData().getURL();
        db = db.substring(db.lastIndexOf("/") + 1);
        List<PGPartitionRuleReplication> replicated = data.stream()
                .map(r -> new PGPartitionRuleReplication(r.getOid(), r.getParchildrelid(), r.getParoid()))
                .collect(Collectors.toList());

        if (replicated != null && !replicated.isEmpty()) {
            pgPartitionRuleRepository.saveAll(replicated);
            logger.info("[pg_partition_rule_rep] Replication complete.");
            writeStatistics((long) replicated.size(), "pg_partition_rule_rep", connection);
        } else {
            logger.info("[pg_partition_rule_rep] Data is empty.");
        }
    }

    private void compareSnapshots(List<PGPartitionRule> newData, Connection connection) throws SQLException {
        List<PGPartitionRuleReplication> replica = pgPartitionRuleRepository.findAll();
        Map<Long, PGPartitionRuleReplication> replicaMap = replica.stream()
                .collect(Collectors.toMap(PGPartitionRuleReplication::getOid, r -> r));

        List<PGPartitionRule> toAdd = new ArrayList<>();
        List<PGPartitionRule> toUpdate = new ArrayList<>();
        List<Long> toDelete = new ArrayList<>(replicaMap.keySet());

        for (PGPartitionRule master : newData) {
            PGPartitionRuleReplication replicaRow = replicaMap.get(master.getOid());
            if (replicaRow == null) {
                toAdd.add(master);
            } else {
                if (!convertToReplication(master).equals(replicaRow)) {
                    toUpdate.add(master);
                }
                toDelete.remove(master.getOid());
            }
        }

        if (!toDelete.isEmpty()) {
            pgPartitionRuleRepository.deleteAllById(toDelete);
            logAction("DELETE", "pg_partition_rule_rep", toDelete.size() 
                    + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            replicate(toAdd, connection);
            logAction("INSERT", "pg_partition_rule_rep", toAdd.size() 
                    + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_partition_rule_rep", toUpdate.size() 
                    + " records updated", "");
        }
    }

    private PGPartitionRuleReplication convertToReplication(PGPartitionRule rule) {
        return new PGPartitionRuleReplication(rule.getOid(), rule.getParchildrelid(), rule.getParoid());
    }

    private long getTransactionCount(Connection connection) throws SQLException {
        String query = "SELECT n_tup_del + n_tup_ins + n_tup_upd AS count " +
                "FROM pg_catalog.pg_stat_all_tables " +
                "WHERE schemaname = 'pg_catalog' AND relname = 'pg_partition_rule'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) return rs.getLong("count");
            connection.close();
        }
        throw new SQLException("Failed to retrieve transaction count");
    }

    private void writeStatistics(Long ops, String table, Connection connection) throws SQLException {
        try (Session session = postgreSessionFactory.openSession()) {
            session.beginTransaction();
            Statistics stat = new Statistics();
            String db = connection.getMetaData().getURL();
            db = db.substring(db.lastIndexOf("/") + 1);
            stat.setDb(db);
            stat.setSchema("pg_catalog");
            stat.setTable_name(table);
            stat.setSum(ops);
            stat.setTimestamp(new Timestamp(System.currentTimeMillis()));
            session.persist(stat);
            session.getTransaction().commit();
        }
        logger.info("[pg_partition_rule] Stats recorded.");
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
