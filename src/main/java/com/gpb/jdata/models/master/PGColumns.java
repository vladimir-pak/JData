package com.gpb.jdata.models.master;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PGColumns {
    private String name;

    private String dataType;

    private int dataTypeLength;

    private String dataTypeDisplay;

    private String description;

    private String constraint;

    private String arrayDataType;
}
