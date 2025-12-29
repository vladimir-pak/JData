package com.gpb.jdata.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class PersistanceTransactions {

    /* ================= ENUM ключи ================= */
    public enum PgKey {

        PG_ATTRIBUTE("PGAttribute", 0L),
        PG_CLASS("PGClass", 0L),
        PG_CONSTRAINT("PGConstraint", 0L),
        PG_NAMESPACE("PGNamespace", 0L),
        PG_DESCRIPTION("PGDescription", 0L),
        PG_TYPE("PGType", 0L),
        PG_PARTITION("PGPartition", 0L),
        PG_ATTRDEF("PGAttrdef", 0L);

        private final String jsonKey;
        private final long defaultValue;

        PgKey(String jsonKey, long defaultValue) {
            this.jsonKey = jsonKey;
            this.defaultValue = defaultValue;
        }

        public String jsonKey() {
            return jsonKey;
        }

        public long defaultValue() {
            return defaultValue;
        }
    }

    /* ================= FIELDS ================= */

    private final ObjectMapper objectMapper =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Value("${greenplum.persistance-storage:config/pg-transactions.json}")
    private String jsonPathStr;

    private Path jsonPath;

    private final ConcurrentMap<PgKey, Long> data = new ConcurrentHashMap<>();
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    /* ================= Инициализация ================= */
    @PostConstruct
    public void init() {
        this.jsonPath = Paths.get(jsonPathStr);

        try {
            if (Files.notExists(jsonPath)) {
                createDefaultFile();
            }

            Map<String, Long> fileData =
                    objectMapper.readValue(
                            jsonPath.toFile(),
                            new TypeReference<Map<String, Long>>() {}
                    );

            for (PgKey key : PgKey.values()) {
                Long value = fileData.get(key.jsonKey());
                if (value != null) {
                    data.put(key, value);
                } else {
                    data.put(key, key.defaultValue());
                    dirty.set(true);
                }
            }

        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize pg-transactions storage", e);
        }
    }

    /* ================= Получение значения по ключу ================= */
    public long get(PgKey key) {
        return data.getOrDefault(key, key.defaultValue());
    }

    /* ================= Запись нового значения по ключу ================= */
    public void put(PgKey key, long value) {
        data.put(key, value);
        dirty.set(true);
    }

    /* ================= Запись в файл на сервере ================= */
    @Scheduled(fixedDelayString = "PT5M") // Каждые 5 мин
    public void flushToFile() {
        if (!dirty.get()) {
            return;
        }

        synchronized (this) {
            if (!dirty.get()) {
                return;
            }

            try {
                Map<String, Long> json = new LinkedHashMap<>();
                for (PgKey key : PgKey.values()) {
                    json.put(
                            key.jsonKey(),
                            data.getOrDefault(key, key.defaultValue())
                    );
                }

                objectMapper.writeValue(jsonPath.toFile(), json);
                dirty.set(false);

            } catch (IOException e) {
                // логируем, но не валим приложение
                e.printStackTrace();
            }
        }
    }

    /* ================= Создание файла json, если отсутствует ================= */
    private void createDefaultFile() throws IOException {
        Files.createDirectories(jsonPath.getParent());

        Map<String, Long> defaults = new LinkedHashMap<>();
        for (PgKey key : PgKey.values()) {
            defaults.put(key.jsonKey(), key.defaultValue());
            data.put(key, key.defaultValue());
        }

        objectMapper.writeValue(jsonPath.toFile(), defaults);
    }

    /* ================= Сохранение файла json перед закрытием ================= */
    @PreDestroy
    public void shutdownSave() {
        flushToFile();
    }
}
