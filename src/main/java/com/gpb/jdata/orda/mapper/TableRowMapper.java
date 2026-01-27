package com.gpb.jdata.orda.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.jdata.orda.dto.TableDTO;
import com.gpb.jdata.orda.dto.TablePartitionDTO;
import com.gpb.jdata.orda.enums.ConstraintType;
import com.gpb.jdata.orda.enums.IntervalType;
import com.gpb.jdata.orda.enums.TableTypes;
import com.gpb.jdata.orda.enums.TypesWithDataLength;
import com.gpb.jdata.orda.dto.ColumnDTO;
import com.gpb.jdata.orda.dto.TableConstraintDTO;
import com.gpb.jdata.orda.properties.OrdProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class TableRowMapper implements RowMapper<TableDTO> {

    private final OrdProperties ordProperties;
    private final ObjectMapper objectMapper;

    @Override
    public TableDTO mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
        TableDTO entity = new TableDTO();

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
                    String tableType = TableTypes.map(tableTypeNode.asText());
                    entity.setTableType(tableType);
                }

                // viewDefinition
                JsonNode viewDefNode = tableData.get("viewDefinition");
                if (viewDefNode != null && !viewDefNode.isNull()) {
                    entity.setViewDefinition(viewDefNode.asText());
                }

                // columns -> List<ColumnDTO>
                JsonNode columnsNode = tableData.get("columns");
                if (columnsNode != null && columnsNode.isArray()) {
                    List<ColumnDTO> columns = objectMapper.readValue(
                            columnsNode.toString(),
                            new TypeReference<List<ColumnDTO>>() {}
                    );
                    List<ColumnDTO> parsedColumns = parseColumns(columns);
                    entity.setColumns(parsedColumns);
                }

                // tableConstraints -> List<TableConstraintDTO>
                JsonNode constraintsNode = tableData.get("tableConstraints");
                if (constraintsNode != null && constraintsNode.isArray()) {
                    List<TableConstraintDTO> tableConstraints = objectMapper.readValue(
                            constraintsNode.toString(),
                            new TypeReference<List<TableConstraintDTO>>() {}
                    );
                    // Предобработка типов constraint для таблицы. Строго в соответствии с enum ConstraintType
                    List<TableConstraintDTO> filteredConstraints = Optional.ofNullable(tableConstraints)
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

                // tablePartition
                JsonNode partitionNode = tableData.get("tablePartition");
                if (partitionNode != null && !partitionNode.isNull()) {
                    // читаем сырой DTO
                    TablePartitionDTO rawPartition = objectMapper.treeToValue(partitionNode, TablePartitionDTO.class);

                    // вычисляем intervalType на основе raw полей
                    IntervalType intervalType = mapIntervalType(
                            rawPartition.getPartitionKind(),
                            rawPartition.getPartitionColumnType(),
                            rawPartition.getInterval(),
                            rawPartition.getColumns()
                    );
                    rawPartition.setIntervalType(intervalType);
                    rawPartition.setPartitionColumnType(null);
                    rawPartition.setPartitionKind(null);

                    entity.setTablePartition(rawPartition);
                }

            } catch (Exception e) {
                throw new RuntimeException("Ошибка преобразования JSONB в DTO", e);
            }
        }

        return entity;
    }

    private List<ColumnDTO> parseColumns(List<ColumnDTO> columns) {
        List<ColumnDTO> parsedColumns = columns.stream()
                .map(column -> {
                    String processedDataType = TypeMapper.mapToOrdaType(column.getDataType());

                    Integer processedDataLength = TypesWithDataLength.getProcessedDataLength(
                            processedDataType,
                            column.getDataLength()
                    );

                    if (processedDataType == "ARRAY") {
                        column.setArrayDataType("UNKNOWN");
                    }

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

    private static IntervalType mapIntervalType(String partitionKind, String columnType, String interval, List<String> columns) {
        if (partitionKind == null) {
            return IntervalType.OTHER;
        }

        // нормализуем
        String kind = partitionKind.toUpperCase(Locale.ROOT);
        String type = columnType != null
                ? columnType.toLowerCase(Locale.ROOT)
                : "";

        // LIST партиционирование
        if ("L".equals(kind)) {
            return IntervalType.COLUMN_VALUE;
        }

        // RANGE партиционирование
        if ("R".equals(kind)) {
            // типы времени → TIME-UNIT
            if (type.contains("timestamp")
                    || type.equals("date")
                    || type.startsWith("time")) {
                return IntervalType.TIME_UNIT;
            }

            // числовые → INTEGER-RANGE
            if (type.equals("int2")
                    || type.equals("int4")
                    || type.equals("int8")
                    || type.startsWith("numeric")) {
                return IntervalType.INTEGER_RANGE;
            }

            return IntervalType.TIME_UNIT;
        }

        // другие варианты (hash и пр.)
        return IntervalType.OTHER;
    }
}
