package com.gpb.jdata.orda.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableConstraints implements Serializable {
    @JsonProperty("columns")
    private List<String> columns;

    @JsonProperty("constraintType")
    private String constraintType;
}
