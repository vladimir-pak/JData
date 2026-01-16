package com.gpb.jdata.orda.dto.lineage;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@NoArgsConstructor 
@AllArgsConstructor 
@Builder
public class LineageEdge {
    private EntityReference fromEntity;
    private EntityReference toEntity;
    private LineageDetails lineageDetails;
}
