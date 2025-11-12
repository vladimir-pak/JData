package com.gpb.jdata.orda.dto;

import com.gpb.jdata.orda.enums.TypesWithDataLength;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ColumnDTO {
    private String name;
    private String dataType;
    private String dataTypeDisplay;
    private String dataLength;
    private String description;
    private String constraint;
    private String ordinalPosition;
    private String arrayDataType;

    /*
     * setter-заглушка с проверкой обязательности заполнения dataLength. Если пусто, то 0
     */
    public void setDataLength(String dataLength) {
        this.dataLength = TypesWithDataLength.getProcessedDataLength(this.dataType, dataLength);
    }
}
