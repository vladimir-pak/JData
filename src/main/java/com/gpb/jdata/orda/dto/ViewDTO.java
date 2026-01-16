package com.gpb.jdata.orda.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ViewDTO {
    private String schemaName;
    private String viewName;
    private String viewDefinition;
}
