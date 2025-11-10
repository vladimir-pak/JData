package com.gpb.jdata.orda.dto;

import java.util.List;

import lombok.Data;

@Data
public class TableDTO {
    private String databaseSchema;
    private String name;
    private String description;
    private Boolean isProjectEntity;
    private String tableType;
    private String viewDefinition;
    private List<ColumnDTO> columns;
    private List<ConstraintDTO> tableConstraints;
    private PartitionDTO tablePartition;
}
