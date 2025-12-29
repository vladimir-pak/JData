package com.gpb.jdata.orda.model;

import com.gpb.jdata.orda.enums.TypesWithDataLength;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ColumnEntity {
    private String name;
    private String dataType;
    private String dataTypeDisplay;
    private String dataLength;
    private String constraint;
    private Integer ordinalPosition;
    private String description;
    private String arrayDataType;

    public void setDataLength(String dataLength) {
        this.dataLength = TypesWithDataLength.getProcessedDataLength(this.dataType, dataLength);
    }
}
