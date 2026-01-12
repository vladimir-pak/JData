package com.gpb.jdata.orda.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnDTO {
    private String name;
    private String dataType;
    private String dataTypeDisplay;
    private Integer dataLength;
    private String constraint;
    private Integer ordinalPosition;
    private String description;
    private String arrayDataType;
    private Integer precision;
    private Integer scale;
}
