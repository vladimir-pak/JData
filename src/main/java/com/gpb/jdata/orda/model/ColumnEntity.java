package com.gpb.jdata.orda.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ColumnEntity extends BaseEntity {
    private String dataType;
    private Integer dataLength;
    private String dataTypeDisplay;
    private String fullyQualifiedName;
    private String constraint;
    private Integer ordinalPosition;
}
