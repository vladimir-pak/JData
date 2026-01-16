package com.gpb.jdata.orda.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.dto.ViewDTO;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class ViewRowMapper implements RowMapper<ViewDTO> {

    @Override
    public ViewDTO mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
        ViewDTO entity = new ViewDTO();

        String viewName = rs.getString("viewname");
        entity.setViewName(viewName);

        String schemaName = rs.getString("schemaname");
        entity.setSchemaName(schemaName);

        String viewDefinition = rs.getString("definition");
        entity.setViewDefinition(viewDefinition);

        return entity;
    }

}
