package com.gpb.jdata.orda.model;

import java.util.List;

import lombok.Data;

@Data
public class PartitionEntity {
    private List<String> columns;
    private String intervalType;
}
