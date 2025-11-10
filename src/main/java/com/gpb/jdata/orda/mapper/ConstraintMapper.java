package com.gpb.jdata.orda.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.dto.ConstraintDTO;
import com.gpb.jdata.orda.model.ConstraintEntity;

public class ConstraintMapper {

    public static ConstraintDTO toDTO(Map<String, Object> map) {
        ConstraintDTO dto = new ConstraintDTO();
        dto.setConstraintType((String) map.get("constrainttype"));
        dto.setColumns(safeCastToStringList(map.get("columns")));
        dto.setReferredColumns(safeCastToStringList(map.get("referredcolumns")));
        return dto;
    }

    public static List<ConstraintDTO> toDTOListFromMap(List<Map<String, Object>> maps) {
        return maps.stream().map(ConstraintMapper::toDTO).collect(Collectors.toList());
    }

    public static ConstraintDTO toDTO(ConstraintEntity entity) {
        ConstraintDTO dto = new ConstraintDTO();
        dto.setConstraintType(entity.getConstraintType());
        dto.setColumns(entity.getColumns());
        dto.setReferredColumns(entity.getReferredColumns());
        return dto;
    }

    public static List<ConstraintDTO> toDTOList(List<ConstraintEntity> entities) {
        return entities.stream().map(ConstraintMapper::toDTO).collect(Collectors.toList());
    }

    public static ConstraintEntity toEntity(ConstraintDTO dto) {
        ConstraintEntity entity = new ConstraintEntity();
        entity.setConstraintType(dto.getConstraintType());
        entity.setColumns(dto.getColumns());
        entity.setReferredColumns(dto.getReferredColumns());
        return entity;
    }

    public static List<ConstraintEntity> toEntityList(List<ConstraintDTO> dtos) {
        return dtos.stream().map(ConstraintMapper::toEntity).collect(Collectors.toList());
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
