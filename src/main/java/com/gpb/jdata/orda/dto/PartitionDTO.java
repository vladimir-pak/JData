package com.gpb.jdata.orda.dto;

import java.util.List;

import lombok.Data;

@Data
public class PartitionDTO {
    private List<String> columns;
    private String intervalType;
}
