package com.gpb.jdata.orda.mapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.lineage.LineageSource;
import com.gpb.jdata.orda.dto.ViewDTO;
import com.gpb.jdata.orda.dto.lineage.AddLineageRequest;
import com.gpb.jdata.orda.dto.lineage.EntityReference;
import com.gpb.jdata.orda.dto.lineage.LineageDetails;
import com.gpb.jdata.orda.dto.lineage.LineageEdge;

import java.util.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class ViewLineageRequestBuilder {

    private final ViewSqlLineageParser parser;
    private final OrdaClient ordaClient;

    public List<AddLineageRequest> buildEdgesForView(
        String omBaseUrl,
        String prefixFqn, // service.db
        ViewDTO viewDTO
    ) {

        String viewFqn = String.format("%s.%s.%s", prefixFqn, viewDTO.getSchemaName(), viewDTO.getViewName());

        String viewId = ordaClient.resolveTableIdByFqn(omBaseUrl, viewFqn)
            .orElse(null);

        if (viewId == null) {
            // если view ещё не создан в OM — lineage строить рано
            log.warn("View {} отсутствует в OMD", viewFqn);
            return List.of();
        }

        Set<ViewSqlLineageParser.TableRef> upstream = parser.extractUpstreamTables(viewDTO.getViewDefinition());

        List<AddLineageRequest> out = new ArrayList<>();

        for (var t : upstream) {
            String schema = (t.schema() == null || t.schema().isBlank()) ? viewDTO.getSchemaName() : t.schema();
            String upstreamFqn = String.format("%s.%s.%s", prefixFqn, schema, t.name());

            String upstreamId = ordaClient.resolveTableIdByFqn(omBaseUrl, upstreamFqn)
                .orElse(null);

            if (upstreamId == null) {
                // upstream таблица может быть не загружена — пропускаем
                log.warn("View upstream {} отсутствует в OMD", upstreamFqn);
                continue;
            }

            AddLineageRequest req = AddLineageRequest.builder()
                .edge(LineageEdge.builder()
                    .fromEntity(EntityReference.builder().type("table").id(upstreamId).build())
                    .toEntity(EntityReference.builder().type("table").id(viewId).build())
                    .lineageDetails(LineageDetails.builder()
                        .source(LineageSource.ViewLineage)
                        .sqlQuery(viewDTO.getViewDefinition()) // можно ограничить длину
                        .build())
                    .build())
                .build();

            out.add(req);
        }

        return out;
    }
}
