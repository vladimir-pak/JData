package com.gpb.jdata.orda.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.dto.SchemaDTO;

@Component
public class SchemaRowMapper implements RowMapper<SchemaDTO> {

    @Override
    public SchemaDTO mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
        return SchemaDTO.builder()
                .name(rs.getString("name"))
                .displayName(rs.getString("name"))
                .description(rs.getString("description"))
                .build();
    }
}
