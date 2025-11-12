package com.gpb.jdata.orda.dto;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PartitionDTO {
    private List<String> columns;
    private String intervalType;
}
