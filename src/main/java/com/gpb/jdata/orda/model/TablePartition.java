package com.gpb.jdata.orda.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gpb.jdata.orda.enums.IntervalType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TablePartition {

    @JsonProperty("columns")
    private List<String> columns;

    @JsonProperty("interval")
    private String interval;

    private IntervalType intervalType;

    @JsonProperty("partitionKind")
    private String partitionKind;        // 'R', 'L', ...

    @JsonProperty("partitionColumnType")
    private String partitionColumnType;  // 'timestamp', 'date', 'int4', ...
}
