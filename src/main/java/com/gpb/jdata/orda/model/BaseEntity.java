package com.gpb.jdata.orda.model;

import java.io.Serializable;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = false)
public class BaseEntity implements Serializable {
    private String name;
    private String description;
}
