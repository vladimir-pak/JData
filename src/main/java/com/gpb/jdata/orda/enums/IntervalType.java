package com.gpb.jdata.orda.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum IntervalType {
    TIME_UNIT("TIME-UNIT"),
    INTEGER_RANGE("INTEGER-RANGE"),
    INGESTION_TIME("INGESTION-TIME"),
    COLUMN_VALUE("COLUMN-VALUE"),
    OTHER("OTHER");

    private final String value;

    IntervalType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
