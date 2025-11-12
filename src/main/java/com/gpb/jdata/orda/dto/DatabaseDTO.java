package com.gpb.jdata.orda.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DatabaseDTO {
    private String name;
    private String description;
}
