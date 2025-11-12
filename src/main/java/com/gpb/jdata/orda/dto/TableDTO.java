package com.gpb.jdata.orda.dto;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableDTO {
    private String databaseSchema;
    private String name;
    private String displayName;
    private String description;
    private Boolean isProjectEntity;
    private String tableType;
    private String viewDefinition;
    private List<ColumnDTO> columns;
    private List<TableConstraintDTO> tableConstraints;
    private PartitionDTO tablePartition;
}
