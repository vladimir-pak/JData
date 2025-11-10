package com.gpb.jdata.orda.mapper;

import com.gpb.jdata.orda.dto.DatabaseDTO;
import com.gpb.jdata.orda.model.DatabaseEntity;

public class DatabaseMapper {
    public static DatabaseDTO toDTO(DatabaseEntity entity) {
        DatabaseDTO dto = new DatabaseDTO();
        dto.setName(entity.getName());
        dto.setDescription(entity.getDescription());
        return dto;
    }
    
    public static DatabaseEntity toEntity(DatabaseDTO dto) {
        DatabaseEntity entity = new DatabaseEntity();
        entity.setName(dto.getName());
        entity.setDescription(dto.getDescription());
        return entity;
    }
}
