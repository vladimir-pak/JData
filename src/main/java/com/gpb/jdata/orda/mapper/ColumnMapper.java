package com.gpb.jdata.orda.mapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.dto.ColumnDTO;
import com.gpb.jdata.orda.enums.TypesWithDataLength;

public class ColumnMapper {

    private static ColumnDTO toDTO(Map<String, Object> map) {
        String processedDataType = TypeMapper.mapToOrdaType(map.get("dtype").toString());
        String processedDataLength = TypesWithDataLength.getProcessedDataLength(
                processedDataType,
                map.get("dtlength").toString()
        );
        
        Integer dataLength = 0;
        if (map.get("dtlength") instanceof Integer) {
            dataLength = (Integer) map.get("dtlength");
        };

        String constraint = null;
        Object notnull = map.get("notnull");
        if (notnull instanceof Boolean && (Boolean) notnull) {
            constraint = "NULLABLE";
        }

        String dataType = map.get("dtype").toString();
        
        return ColumnDTO.builder()
                .name(map.get("columnname").toString())
                .dataType(dataType)
                .arrayDataType(resolveArrayType(dataType, processedDataType))
                .dataTypeDisplay(dataTypeDisplay(dataType, dataLength))
                .dataLength(processedDataLength)
                .description(map.get("description").toString())
                .constraint(constraint)
                .ordinalPosition(map.get("attnum").toString())
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
