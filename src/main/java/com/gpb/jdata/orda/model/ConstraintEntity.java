package com.gpb.jdata.orda.model;

import java.util.List;

import lombok.Data;

@Data
public class ConstraintEntity {
    private String constraintType;
    private List<String> columns;
    private List<String> referredColumns;
}
