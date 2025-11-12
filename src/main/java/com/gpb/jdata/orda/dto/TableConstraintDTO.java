package com.gpb.jdata.orda.dto;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableConstraintDTO {
    private List<String> columns;
    private List<String> referredColumns;
    private String constraintType;
}
