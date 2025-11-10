package com.gpb.jdata.orda.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.dto.PartitionDTO;
import com.gpb.jdata.orda.model.PartitionEntity;

public class PartitionMapper {
    public static PartitionDTO toDTO(Map<String, Object> map) {
        PartitionDTO dto = new PartitionDTO();
        dto.setColumns(safeCastToStringList(map.get("columns")));
        dto.setIntervalType((String) map.get("intervaltype"));
        return dto;
    }

    public static PartitionDTO toDTO(PartitionEntity entity) {
        PartitionDTO dto = new PartitionDTO();
        dto.setColumns(entity.getColumns());
        dto.setIntervalType(entity.getIntervalType());
        return dto;
    }

    public static PartitionEntity toEntity(PartitionDTO dto) {
        PartitionEntity entity = new PartitionEntity();
        entity.setColumns(dto.getColumns());
        entity.setIntervalType(dto.getIntervalType());
        return entity;
    }

    private static List<String> safeCastToStringList(Object obj) {
        if (obj instanceof List) {
            return ((List<?>) obj).stream()
                .filter(item -> item == null || item instanceof String)
                .map(item -> (String) item)
                .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}
