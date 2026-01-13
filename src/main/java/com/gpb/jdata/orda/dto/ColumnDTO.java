package com.gpb.jdata.orda.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonInclude(JsonInclude.Include.NON_NULL)
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
