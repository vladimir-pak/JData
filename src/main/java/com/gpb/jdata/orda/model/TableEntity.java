package com.gpb.jdata.orda.model;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TableEntity extends BaseEntity {
    private String databaseSchema;
    private String tableType;
    private String viewDefinition;
    private Boolean isProjectEntity;
    private List<ColumnEntity> columns;
    private List<ConstraintEntity> constraints;
    private PartitionEntity partition;
}
