package com.gpb.jdata.orda.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TableEntity {
    private String name;
    private String displayName;
    private String databaseSchema;
    private String description;
    private String tableType;
    private Boolean isProjectEntity;
    private String viewDefinition;
    private List<ColumnEntity> columns;
    private List<TableConstraints> tableConstraints;

    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();
    
    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        additionalProperties.put(name, value);
    }
}