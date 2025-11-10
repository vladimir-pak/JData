package com.gpb.jdata.orda.mapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.dto.ColumnDTO;
import com.gpb.jdata.orda.model.ColumnEntity;

public class ColumnMapper {
    public static ColumnDTO toDTO(Map<String, Object> map) {
        ColumnDTO dto = new ColumnDTO();
        dto.setName((String) map.get("columnname"));
        dto.setDataType((String) map.get("datatype"));
        dto.setDataLength((Integer) map.get("datalength"));
        dto.setDataTypeDisplay((String) map.get("datatypedisplay"));
        dto.setDescription((String) map.get("description"));
        dto.setFullyQualifiedName((String) map.get("fullyqualifiedname"));
        dto.setConstraint((String) map.get("constraint"));
        dto.setOrdinalPosition((Integer) map.get("ordinalposition"));
        return dto;
    }

    public static List<ColumnDTO> toDTOListFromMap(List<Map<String, Object>> maps) {
        return maps.stream().map(ColumnMapper::toDTO).collect(Collectors.toList());
    }

    public static ColumnDTO toDTO(ColumnEntity entity) {
        ColumnDTO dto = new ColumnDTO();
        dto.setName(entity.getName());
        dto.setDataType(entity.getDataType());
        dto.setDataLength(entity.getDataLength());
        dto.setDataTypeDisplay(entity.getDataTypeDisplay());
        dto.setDescription(entity.getDescription());
        dto.setFullyQualifiedName(entity.getFullyQualifiedName());
        dto.setConstraint(entity.getConstraint());
        dto.setOrdinalPosition(entity.getOrdinalPosition());
        return dto;
    }

    public static List<ColumnDTO> toDTOList(List<ColumnEntity> entities) {
        return entities.stream().map(ColumnMapper::toDTO).collect(Collectors.toList());
    }

    public static ColumnEntity toEntity(ColumnDTO dto) {
        ColumnEntity entity = new ColumnEntity();
        entity.setName(dto.getName());
        entity.setDataType(dto.getDataType());
        entity.setDataLength(dto.getDataLength());
        entity.setDataTypeDisplay(dto.getDataTypeDisplay());
        entity.setDescription(dto.getDescription());
        entity.setFullyQualifiedName(dto.getFullyQualifiedName());
        entity.setConstraint(dto.getConstraint());
        entity.setOrdinalPosition(dto.getOrdinalPosition());
        return entity;
    }
    
    public static List<ColumnEntity> toEntityList(List<ColumnDTO> dtos) {
        return dtos.stream().map(ColumnMapper::toEntity).collect(Collectors.toList());
    }
}
