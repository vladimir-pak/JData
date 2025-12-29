package com.gpb.jdata.orda.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.jdata.orda.enums.TypesWithDataLength;
import com.gpb.jdata.orda.model.ColumnEntity;
import com.gpb.jdata.orda.model.ConstraintType;
import com.gpb.jdata.orda.model.TableConstraints;
import com.gpb.jdata.orda.model.TableEntity;
import com.gpb.jdata.orda.properties.OrdProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class TableRowMapper implements RowMapper<TableEntity> {

    private final OrdProperties ordProperties;
    private final ObjectMapper objectMapper;

    @Override
    public TableEntity mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
        TableEntity entity = new TableEntity();

        // name / displayName
        String tableName = rs.getString("table_name");
        entity.setName(tableName);
        entity.setDisplayName(tableName);

        // FQN схемы
        String databaseSchema = String.format(
                "%s.%s",
                ordProperties.getPrefixFqn(),
                rs.getString("schema_name")
        );
        entity.setDatabaseSchema(databaseSchema);

        // описание таблицы (obj_description)
        entity.setDescription(rs.getString("description"));

        // всегда false
        entity.setIsProjectEntity(false);

        // JSONB -> DTO
        String jsonData = rs.getString("table_structure");
        if (jsonData != null) {
            try {
                JsonNode tableData = objectMapper.readTree(jsonData);

                // tableType
                JsonNode tableTypeNode = tableData.get("tableType");
                if (tableTypeNode != null && !tableTypeNode.isNull()) {
                    entity.setTableType(tableTypeNode.asText());
                }

                // viewDefinition
                JsonNode viewDefNode = tableData.get("viewDefinition");
                if (viewDefNode != null && !viewDefNode.isNull()) {
                    entity.setViewDefinition(viewDefNode.asText());
                }

                // columns -> List<ColumnEntity>
                JsonNode columnsNode = tableData.get("columns");
                if (columnsNode != null && columnsNode.isArray()) {
                    List<ColumnEntity> columns = objectMapper.readValue(
                            columnsNode.toString(),
                            new TypeReference<List<ColumnEntity>>() {}
                    );
                    List<ColumnEntity> parsedColumns = parseColumns(columns);
                    entity.setColumns(parsedColumns);
                }

                // tableConstraints -> List<TableConstraints>
                JsonNode constraintsNode = tableData.get("tableConstraints");
                if (constraintsNode != null && constraintsNode.isArray()) {
                    List<TableConstraints> tableConstraints = objectMapper.readValue(
                            constraintsNode.toString(),
                            new TypeReference<List<TableConstraints>>() {}
                    );
                    // Предобработка типов constraint для таблицы. Строго в соответствии с enum ConstraintType
                    List<TableConstraints> filteredConstraints = Optional.ofNullable(tableConstraints)
                            .orElseGet(List::of)
                            .stream()
                            .filter(c -> {
                                boolean keep = ConstraintType.isSupported(c.getConstraintType());
                                if (!keep) {
                                    log.debug("Constraint '{}' пропущен — не поддерживается Ордой",
                                            c.getConstraintType());
                                }
                                return keep;
                            })
                            .collect(Collectors.toList());
                    entity.setTableConstraints(filteredConstraints);
                }

            } catch (Exception e) {
                throw new RuntimeException("Ошибка преобразования JSONB в DTO", e);
            }
        }

        return entity;
    }

    private List<ColumnEntity> parseColumns(List<ColumnEntity> columns) {
        List<ColumnEntity> parsedColumns = columns.stream()
                .map(column -> {
                    String processedDataType = TypeMapper.mapToOrdaType(column.getDataType());

                    String processedDataLength = TypesWithDataLength.getProcessedDataLength(
                            processedDataType,
                            column.getDataLength()
                    );

                    String constraint = column.getConstraint();
                    if ("NULLABLE".equalsIgnoreCase(constraint)) {
                        constraint = null;
                    }

                    column.setDataType(processedDataType);
                    column.setDataLength(processedDataLength);
                    column.setConstraint(constraint);

                    return column;
                })
                .collect(Collectors.toList());
        return parsedColumns;
    }
}
