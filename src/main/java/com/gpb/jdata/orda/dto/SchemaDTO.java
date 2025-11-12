package com.gpb.jdata.orda.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SchemaDTO {
    private String database;
    private String name;
    private String displayName;
    private String description;
}
