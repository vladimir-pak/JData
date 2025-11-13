package com.gpb.jdata.orda.model;

import jakarta.persistence.Entity;
import lombok.Data;

@Data
@Entity
public class SchemaEntity {
    private String oid;
    private String name;
    private String description;
}
