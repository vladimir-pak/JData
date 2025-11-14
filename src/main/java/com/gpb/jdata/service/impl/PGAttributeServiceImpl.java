package com.gpb.jdata.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.gpb.jdata.config.DatabaseConfig;
import com.gpb.jdata.log.SvoiCustomLogger;
import com.gpb.jdata.log.SvoiSeverityEnum;
import com.gpb.jdata.models.master.PGAttribute;
import com.gpb.jdata.models.master.PGAttributeId;
import com.gpb.jdata.models.replication.Action;
import com.gpb.jdata.models.replication.PGAttributeReplication;
import com.gpb.jdata.models.replication.Statistics;
import com.gpb.jdata.repository.ActionRepository;
import com.gpb.jdata.repository.PGAttributeRepository;
import com.gpb.jdata.service.PGAttributeService;
import com.gpb.jdata.utils.IteratorStringInputStream;
import com.gpb.jdata.utils.ResultSetIterator;
import com.gpb.jdata.utils.diff.NamespaceDiffContainer;

import lombok.RequiredArgsConstructor;

/**
* pg_attribute service provides creation of master data from pg_catalog.pg_attribute
* and inserting, deleting or updating it in the replica tables.
 */
@Service
@RequiredArgsConstructor
public class PGAttributeServiceImpl implements PGAttributeService {
    private static final Logger logger = LoggerFactory.getLogger(PGAttributeService.class);
    private final SvoiCustomLogger svoiLogger;

    private final PGAttributeRepository pgAttributeRepository;
    private final ActionRepository actionRepository;
    private final DatabaseConfig databaseConfig;
    private final SessionFactory postgreSessionFactory;

    private final NamespaceDiffContainer diffContainer;

    private long lastTransactionCount = 0;

    /**
     * Создание начального снапшота и запись данных в таблицу репликации
     * Для инициализирующей загрузки используется иной способ репликации - через copy from
     */
    @Override
    public void initialSnapshot() throws SQLException {
        svoiLogger.send(
                "startInitSnapshot",
                "Start PGAttribute init",
                "Started PGAttribute init",
                SvoiSeverityEnum.ONE);

        try (Connection connection = databaseConfig.getConnection()) {
            logger.info("[pg_attribute] Starting initial snapshot (streaming COPY)...");
            String query = """
                    SELECT attrelid, attnum, attname, atthasdef, attnotnull, atttypid, atttypmod
                    FROM pg_catalog.pg_attribute
                    """;

            long inserted = streamCopyFromResultSet(query, connection, this::serializeRowFromResultSet);

            writeStatistics(inserted, "pg_attribute_rep", connection);
            logAction("INITIAL_SNAPSHOT", "pg_attribute", 
                    inserted + " records added", "");

            logger.info("[pg_attribute] Initial snapshot finished. Inserted: {}", inserted);
        } catch (SQLException | IOException e) {
            logger.error("[pg_attribute] Error during initial snapshot", e);
            throw (e instanceof SQLException) ? (SQLException) e : 
                    new SQLException("Initial snapshot failed", e);
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
			"Start PGAttribute sync", 
			"Started PGAttribute sync", 
			SvoiSeverityEnum.ONE);
        try (Connection connection = databaseConfig.getConnection()) {
            long currentTransactionCount = getTransactionCountMain(connection);

            if (currentTransactionCount == lastTransactionCount) {
                logger.info("[pg_attribute] {} No changes detected. Skipping synchronization.", 
                        currentTransactionCount);
                return;
            }

            long diff = currentTransactionCount - lastTransactionCount;
            logger.info("[pg_attribute] {} Changes detected. Starting synchronization...", diff);

            List<PGAttribute> newData = readMasterData(connection);
            compareSnapshots(newData, connection);
            lastTransactionCount = currentTransactionCount;
            writeStatistics(currentTransactionCount, "pg_attribute", connection);
        } catch (SQLException e) {
            logger.error("[pg_attribute] Error during synchronization", e);
        }
    }

    @Override
    @Async
    public CompletableFuture<Void> synchronizeAsync() {
        synchronize();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Чтение данных из таблицы pg_attribute
     */
    @Override
    public List<PGAttribute> readMasterData(Connection connection) throws SQLException {
        List<PGAttribute> masterData = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_catalog.pg_attribute")) {
            while (resultSet.next()) {
                masterData.add(new PGAttribute(
                        resultSet.getLong("attrelid"),
                        resultSet.getLong("attnum"),
                        resultSet.getString("attname"),
                        resultSet.getBoolean("atthasdef"),
                        resultSet.getBoolean("attnotnull"),
                        resultSet.getLong("atttypid"),
                        resultSet.getInt("atttypmod")
                ));
            }
        }
        return masterData;
    }

    /**
     * Выполняет streaming COPY: читает ResultSet по query и посылает строки в COPY без удерживания всего набора в памяти.
     *
     * @return количество вставленных строк (CopyManager возвращает long)
     */
    private long streamCopyFromResultSet(String query, Connection connection, Function<ResultSet, String> serializer)
            throws SQLException, IOException {

        Statement stmt = null;
        ResultSet rs = null;
        InputStream inputStream = null;

        try {
            // Используем forward-only и fetchSize для стриминга больших наборов
            stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // Для PostgreSQL fetchSize > 0 переключает серверный курсор и позволяет стримить
            stmt.setFetchSize(10_000);

            rs = stmt.executeQuery(query);

            Iterator<ResultSet> rsIterator = new ResultSetIterator(rs);
            // Преобразуем итератор ResultSet в Iterator<String> через serializer (строка в формате COPY)
            Iterator<String> lineIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return rsIterator.hasNext();
                }

                @Override
                public String next() {
                    ResultSet r = rsIterator.next();
                    return serializer.apply(r);
                }
            };

            inputStream = new IteratorStringInputStream(lineIterator);

            PGConnection pgConnection = connection.unwrap(PGConnection.class);
            CopyManager copyManager = pgConnection.getCopyAPI();

            String sql = """
                    COPY jdata.pg_attribute_rep (attrelid, attnum, attname, atthasdef, 
                    attnotnull, atttypid, atttypmod)
                    FROM STDIN WITH (FORMAT text)
                    """;

            logger.info("[pg_attribute_rep] Starting COPY FROM STDIN...");
            long rows = copyManager.copyIn(sql, inputStream);
            logger.info("[pg_attribute_rep] COPY finished, rows inserted: {}", rows);

            return rows;

        } finally {
            // Закрываем InputStream прежде чем закрыть ResultSet/Statement.
            // CopyManager.copyIn прочитает весь inputStream до конца (или бросит исключение).
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ex) {
                    logger.warn("Failed to close input stream after COPY", ex);
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.warn("Failed to close ResultSet", ex);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    logger.warn("Failed to close Statement", ex);
                }
            }
        }
    }

    /**
     * Сериализация одной строки для COPY в текстовом формате (TAB-separated).
     * Формат: col1\tcol2\t...\tcolN\n
     * NULL -> \N
     * boolean -> t/f
     */
    private String serializeRowFromResultSet(ResultSet rs) {
        try {
            long attrelid = rs.getLong("attrelid");
            long attnum = rs.getLong("attnum");
            String attname = rs.getString("attname");
            boolean atthasdef = rs.getBoolean("atthasdef");
            boolean attnotnull = rs.getBoolean("attnotnull");
            long atttypid = rs.getLong("atttypid");
            int atttypmod = rs.getInt("atttypmod");

            StringBuilder sb = new StringBuilder(128);
            sb.append(escape(attrelid)).append('\t');
            sb.append(escape(attnum)).append('\t');
            sb.append(escape(attname)).append('\t');
            sb.append(atthasdef ? "t" : "f").append('\t');
            sb.append(attnotnull ? "t" : "f").append('\t');
            sb.append(escape(atttypid)).append('\t');
            sb.append(escape(atttypmod)).append('\n');

            return sb.toString();
        } catch (SQLException e) {
            logger.error("Error serializing ResultSet row", e);
            throw new RuntimeException(e);
        }
    }

    private String escape(Object v) {
        if (v == null) return "\\N";
        String s = String.valueOf(v);
        // Простейшее экранирование для COPY text
        return s.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    /**
     * Репликация данных в таблицу репликации
     */
    private void replicate(List<PGAttribute> data, Connection connection) throws SQLException {    
        List<PGAttributeReplication> replicationData = data.parallelStream()
                .map(d -> convertToReplication(d, "adb"))
                .collect(Collectors.toList());

        if (replicationData != null && !replicationData.isEmpty()) {
            pgAttributeRepository.saveAll(replicationData);
            logger.info("[pg_attribute_rep] Data replicated successfully.");
            writeStatistics((long) replicationData.size(), "pg_attribute_rep", connection);
        } else {
            logger.info("[pg_attribute_rep] Data is empty.");
        }
    }

    /**
     * Сравнение снапшотов
     */
    private void compareSnapshots(List<PGAttribute> newData, Connection connection) throws SQLException {
        List<PGAttributeReplication> replicationData = pgAttributeRepository.findAll();
        Map<PGAttributeId, PGAttributeReplication> replicationMap = replicationData.parallelStream()
                .collect(Collectors.toMap(PGAttributeReplication::getId, r -> r));

        List<PGAttribute> toAdd = new ArrayList<>();
        List<PGAttribute> toUpdate = new ArrayList<>();
        List<PGAttributeId> toDelete = new ArrayList<>(replicationMap.keySet());

        newData.parallelStream().forEach( masterRecord -> {
            PGAttributeId id = new PGAttributeId(masterRecord.getAttrelid(), masterRecord.getAttname());
            PGAttributeReplication replicationRecord = replicationMap.get(id);

            if (replicationRecord == null) {
                toAdd.add(masterRecord);
            } else {
                if (!convertToReplication(masterRecord, "adb").equals(replicationRecord)) {
                    toUpdate.add(masterRecord);
                }
                toDelete.remove(id);
            }
        });

        if (!toDelete.isEmpty()) {
            logger.info("[pg_attribute_rep] Deleting {} records from the replica table", toDelete.size());
            pgAttributeRepository.deleteAllById(toDelete);
            toDelete.forEach(e -> {
                    if (!diffContainer.containsInDeletedOids(e.getAttrelid())) {
                        diffContainer.addUpdated(e.getAttrelid());
                    }
            });
            logAction("DELETE", "pg_attribute_rep", toDelete.size() + " records deleted", "");
        }

        if (!toAdd.isEmpty()) {
            logger.info("[pg_attribute_rep] Adding {} records to the replica table", toAdd.size());
            replicate(toAdd, connection);
            toAdd.forEach(e -> diffContainer.addUpdated(e.getAttrelid()));
            logAction("INSERT", "pg_attribute_rep", toAdd.size() + " records inserted", "");
        }

        if (!toUpdate.isEmpty()) {
            logger.info("[pg_attribute_rep] Updating {} records in the replica table", toUpdate.size());
            toUpdate.forEach(e -> {
                    logAction("UPDATE", "pg_attribute_rep", " old: " +
                                    pgAttributeRepository.findPGAttributeReplicationById(new PGAttributeId(e.getAttrelid(), e.getAttname()))
                            , " new:" + e.toString());
                    diffContainer.addUpdated(e.getAttrelid());
            });
            replicate(toUpdate, connection);
            logAction("UPDATE", "pg_attribute_rep", toUpdate.size() + " records updated", "");
        }

        long totalOperations = toAdd.size() + toUpdate.size() + toDelete.size();
        writeStatistics(totalOperations, "pg_attribute_rep", connection);
    }

    /**
     * Получение количества операций для таблицы pg_attribute
     */
    private long getTransactionCountMain(Connection connection) throws SQLException {
        String query = """
                SELECT n_tup_del + n_tup_ins + n_tup_upd as count 
                FROM pg_catalog.pg_stat_all_tables WHERE schemaname = ? AND relname = ?
                """;

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "pg_catalog");
            statement.setString(2, "pg_attribute");
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
    private PGAttributeReplication convertToReplication(PGAttribute pgAttribute, String db) {
        PGAttributeId id = new PGAttributeId(pgAttribute.getAttrelid(), pgAttribute.getAttname());
        PGAttributeReplication replication = new PGAttributeReplication();
        replication.setId(id);
        replication.setAttnum(pgAttribute.getAttnum());
        replication.setAtthasdef(pgAttribute.isAtthasdef());
        replication.setAttnotnull(pgAttribute.isAttnotnull());
        replication.setAtttypid(pgAttribute.getAtttypid());
        replication.setAtttypmod(pgAttribute.getAtttypmod());
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
        logger.info("[pg_attribute] Statistics written for table {}", name);
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
