package com.gpb.jdata.orda;

import java.util.Map;

public interface OrdaClient {
    // 1) База данных
    boolean checkDatabaseExists(String databaseName);          // GET /v1/databases/name/{databaseName}
    void createDatabase(String databaseName);                  // PUT /v1/databases

    // 2) Схемы
    void createOrUpdateSchema(Map<String, Object> body);   // PUT /v1/databaseSchemas
    boolean checkSchemaExists(String fqn);                 // GET /v1/databaseSchemas/name/{fqn}
    void deleteSchema(String fqn);                         // DELETE /v1/databaseSchemas/name/{fqn}
    
    // 3) Таблицы
    void createOrUpdateTable(Map<String, Object> body);   // PUT /v1/tables
    boolean checkTableExists(String fqn);                 // GET /v1/tables/name/{fqn}
    boolean isProjectEntity(String fqn);                  // GET /v1/tables/name/{fqn} -> parse isProjectEntity
    void deleteTable(String fqn);                         // DELETE /v1/tables/name/{fqn}
}
