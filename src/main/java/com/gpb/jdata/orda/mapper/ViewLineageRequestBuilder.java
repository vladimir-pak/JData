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

    // Схемы для резолвинга upstream таблиц, если не нашли в схеме view
    private static final List<String> DEFAULT_SCHEMA_FALLBACK =
            List.of("pg_catalog", "information_schema");

    public List<AddLineageRequest> buildEdgesForView(
        String omBaseUrl,
        String prefixFqn, // service.db
        ViewDTO viewDTO
    ) {
        String viewFqn = String.format("%s.%s.%s", prefixFqn, viewDTO.getSchemaName(), viewDTO.getViewName());

        // Кэш на один вызов, чтобы не дергать resolveTableIdByFqn много раз
        Map<String, Optional<String>> idCache = new HashMap<>();

        String viewId = resolveIdCached(omBaseUrl, viewFqn, idCache).orElse(null);
        if (viewId == null) {
            log.warn("View {} отсутствует в OMD", viewFqn);
            return List.of();
        }

        Set<ViewSqlLineageParser.TableRef> upstream =
                parser.extractUpstreamTables(viewDTO.getViewDefinition());

        List<AddLineageRequest> out = new ArrayList<>(upstream.size());

        for (var t : upstream) {

            Optional<String> upstreamIdOpt = resolveUpstreamId(
                    omBaseUrl,
                    prefixFqn,
                    viewDTO.getSchemaName(),
                    t,
                    idCache
            );

            if (upstreamIdOpt.isEmpty()) {
                // upstream таблица может быть не загружена — пропускаем
                String debugName = (t.schema() == null ? "" : (t.schema() + ".")) + t.name();
                log.warn("View upstream {} не найден в OMD (schemaView/pg_catalog/information_schema)", debugName);
                continue;
            }

            String upstreamId = upstreamIdOpt.get();

            out.add(AddLineageRequest.builder()
                    .edge(LineageEdge.builder()
                            .fromEntity(EntityReference.builder().type("table").id(upstreamId).build())
                            .toEntity(EntityReference.builder().type("table").id(viewId).build())
                            .lineageDetails(LineageDetails.builder()
                                    .source(LineageSource.ViewLineage)
                                    .sqlQuery(viewDTO.getViewDefinition())
                                    .build())
                            .build())
                    .build());
        }

        return out;
    }

    private Optional<String> resolveUpstreamId(
            String omBaseUrl,
            String prefixFqn,          // service.db
            String viewSchema,
            ViewSqlLineageParser.TableRef ref,
            Map<String, Optional<String>> idCache
    ) {
        String tableName = ref.name();

        // 1) если схема указана в SQL — используем её
        if (ref.schema() != null && !ref.schema().isBlank()) {
            String fqn = String.format("%s.%s.%s", prefixFqn, ref.schema(), tableName);
            return resolveIdCached(omBaseUrl, fqn, idCache);
        }

        // 2) иначе пробуем по порядку: schemaView → pg_catalog → information_schema
        List<String> candidates = new ArrayList<>(1 + DEFAULT_SCHEMA_FALLBACK.size());
        candidates.add(viewSchema);
        candidates.addAll(DEFAULT_SCHEMA_FALLBACK);

        for (String schemaCandidate : candidates) {
            String fqn = String.format("%s.%s.%s", prefixFqn, schemaCandidate, tableName);
            Optional<String> id = resolveIdCached(omBaseUrl, fqn, idCache);
            if (id.isPresent()) return id;
        }

        return Optional.empty();
    }

    private Optional<String> resolveIdCached(
            String omBaseUrl,
            String fqn,
            Map<String, Optional<String>> idCache
    ) {
        return idCache.computeIfAbsent(fqn, key ->
                ordaClient.resolveTableIdByFqn(omBaseUrl, key)
        );
    }
}
