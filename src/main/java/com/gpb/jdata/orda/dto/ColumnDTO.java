package com.gpb.jdata.orda.dto;

import lombok.Data;

@Data
public class ColumnDTO {
    private String name;
    private String dataType;
    private Integer dataLength;
    private String dataTypeDisplay;
    private String description;
    private String fullyQualifiedName;
    private String constraint;
    private Integer ordinalPosition;
}
