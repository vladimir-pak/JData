package com.gpb.jdata.orda.mapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.dto.ColumnDTO;
import com.gpb.jdata.orda.enums.TypesWithDataLength;

public class ColumnMapper {

    private static ColumnDTO toDTO(Map<String, Object> map) {
        String dtype = map.get("dtype") != null ? map.get("dtype").toString() : null;
        String dtlength = map.get("dtlength") != null ? map.get("dtlength").toString() : null;
        String columnname = map.get("columnname") != null ? map.get("columnname").toString() : null;
        String description = map.get("description") != null ? map.get("description").toString() : null;
        String attnum = map.get("attnum") != null ? map.get("attnum").toString() : null;
        
        String processedDataType = TypeMapper.mapToOrdaType(dtype);
        String processedDataLength = TypesWithDataLength.getProcessedDataLength(
                processedDataType,
                dtlength
        );
        
        Integer dataLength = 0;
        if (map.get("dtlength") instanceof Integer) {
            dataLength = (Integer) map.get("dtlength");
        };

        String constraint = null;
        Object notnull = map.get("notnull");
        if (notnull instanceof Boolean && (Boolean) notnull) {
            constraint = "NOT_NULL";
        }
        
        return ColumnDTO.builder()
                .name(columnname)
                .dataType(processedDataType)
                .arrayDataType(resolveArrayType(dtype, processedDataType))
                .dataTypeDisplay(dataTypeDisplay(processedDataType, dataLength))
                .dataLength(processedDataLength)
                .description(description)
                .constraint(constraint)
                .ordinalPosition(attnum)
                .build();
    }

    private static String resolveArrayType(String sourceType, String processedType) {
        if (sourceType == null || !"ARRAY".equalsIgnoreCase(processedType)) {
            return null; // только для ARRAY
        }
        if (sourceType.endsWith("[]")) {
            return sourceType.substring(0, sourceType.length() - 2).toUpperCase();
        }
        return null;
    }

    public static List<ColumnDTO> toDTOListFromMap(List<Map<String, Object>> maps) {
        return maps.stream().map(ColumnMapper::toDTO).collect(Collectors.toList());
    }

    public static String dataTypeDisplay(String pgTypName, Integer atttypmod) {
        String base = (pgTypName == null ? "" : pgTypName).toLowerCase();
        if (atttypmod != null && atttypmod > 0) {
            return base + "(" + atttypmod + ")";
        }
        return base;
    }
}
