package com.gpb.jdata.orda.dto;

import java.util.List;

import lombok.Data;

@Data
public class ConstraintDTO {
    private String constraintType;
    private List<String> columns;
    private List<String> referredColumns;
}
