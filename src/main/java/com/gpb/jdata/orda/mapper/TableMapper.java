package com.gpb.jdata.orda.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.dto.ColumnDTO;
import com.gpb.jdata.orda.dto.PartitionDTO;
import com.gpb.jdata.orda.dto.TableConstraintDTO;
import com.gpb.jdata.orda.dto.TableDTO;
import com.gpb.jdata.orda.repository.TableRepository;

public class TableMapper {
    public static TableDTO toRequestBody(List<Map<String, Object>> rows,
                                                    List<Map<String, Object>> partitions,
                                                    TableRepository tableRepository,
                                                    String database) {
        Map<String, Object> first = rows.get(0);

        String schema = String.format("%s.%s", 
                database, (String) first.get("dbschema"));
        String table  = (String) first.get("tablename");
        String tableType = (String) first.get("tabletype");

        List<TableConstraintDTO> tableConstraints = new ArrayList<>();
        
        Object pkConstraintObj = first.get("pk_constraint");
        if (pkConstraintObj != null && pkConstraintObj instanceof String[]) {
            TableConstraintDTO pkCon = TableConstraintDTO.builder()
                            .columns(Arrays.asList((String[]) pkConstraintObj))
                            .build();
            tableConstraints.add(pkCon);
        }

        Object fkConstraintObj = first.get("fk_constraint");
        if (fkConstraintObj != null && fkConstraintObj instanceof String[]) {
            TableConstraintDTO fkCon = TableConstraintDTO.builder()
                            .columns(Arrays.asList((String[]) fkConstraintObj))
                            .build();
            tableConstraints.add(fkCon);
        }

        PartitionDTO tablePartition = null;
        if (!partitions.isEmpty()) {
            String intervalType = (String) partitions.get(0).get("partition_strategy");
            List<String> partCols = partitions.stream()
                    .map(p -> (String) p.get("attname"))
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());
            tablePartition = PartitionDTO.builder()
                    .columns(partCols)
                    .intervalType(intervalType)
                    .build();
        }

        List<ColumnDTO> columns = ColumnMapper.toDTOListFromMap(rows);

        TableDTO body = TableDTO.builder()
                .columns(columns)
                .tableConstraints(tableConstraints)
                .databaseSchema(schema)
                .description((String) first.get("table_description"))
                .displayName(table)
                .name(table)
                .tableType(tableType)
                .viewDefinition((String) first.get("view_definition"))
                .isProjectEntity(false)
                .tablePartition(tablePartition)
                .build();

        
        return body;
    }
}
