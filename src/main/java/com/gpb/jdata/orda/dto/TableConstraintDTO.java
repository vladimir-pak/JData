package com.gpb.jdata.orda.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableConstraintDTO {
    @JsonProperty("columns")
    private List<String> columns;

    @JsonProperty("constraintType")
    private String constraintType;
}
