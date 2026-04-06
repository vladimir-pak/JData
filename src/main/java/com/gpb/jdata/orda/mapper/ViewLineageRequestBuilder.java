package com.gpb.jdata.orda.mapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.lineage.LineageSource;
import com.gpb.jdata.orda.dto.ViewDTO;
import com.gpb.jdata.orda.dto.lineage.AddLineageRequest;
import com.gpb.jdata.orda.dto.lineage.ColumnsLineage;
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
        String normalizedPrefixFqn = normalizeIdent(prefixFqn);
        String viewSchema = normalizeIdent(viewDTO.getSchemaName());
        String viewName = normalizeIdent(viewDTO.getViewName());
        String sql = viewDTO.getViewDefinition();

        if (normalizedPrefixFqn == null || normalizedPrefixFqn.isBlank()
                || viewSchema == null || viewSchema.isBlank()
                || viewName == null || viewName.isBlank()) {
            log.warn(
                    "Некорректные входные данные: prefixFqn='{}', schemaName='{}', viewName='{}'",
                    prefixFqn,
                    viewDTO.getSchemaName(),
                    viewDTO.getViewName()
            );
            return List.of();
        }

        String viewFqn = String.format("%s.%s.%s", normalizedPrefixFqn, viewSchema, viewName);

        // Кэш на один вызов: tableFqn -> Optional<tableId>
        Map<String, Optional<String>> idCache = new HashMap<>();

        String viewId = resolveIdCached(omBaseUrl, viewFqn, idCache).orElse(null);
        if (viewId == null) {
            log.warn("View {} отсутствует в OMD", viewFqn);
            return List.of();
        }

        if (sql == null || sql.isBlank()) {
            log.warn("View {}: viewDefinition пустой, пропускаем lineage", viewFqn);
            return List.of();
        }

        ViewSqlLineageParser.ParsedLineage parsed = parser.parse(sql);

        if (parsed.upstreamTables().isEmpty()) {
            log.debug("View {}: upstream tables не найдены", viewFqn);
            return List.of();
        }

        // alias/name(lower) -> resolved upstream tableFqn
        Map<String, String> aliasToResolvedFqn = new HashMap<>();

        // resolved upstream tableFqn -> index columnName(lower) -> columnFqn
        // здесь columnFqn строим best-effort: tableFqn + "." + col
        Map<String, Map<String, String>> upstreamColumnIndexByTableFqn = new HashMap<>();

        // 1) Резолвим alias/name -> upstream tableFqn
        for (var e : parsed.aliasToTable().entrySet()) {
            String aliasKey = normKey(e.getKey());
            if (aliasKey == null) {
                continue;
            }

            Optional<String> upstreamFqnOpt = resolveUpstreamTableFqn(
                    omBaseUrl,
                    normalizedPrefixFqn,
                    viewSchema,
                    e.getValue(),
                    idCache
            );

            if (upstreamFqnOpt.isEmpty()) {
                continue;
            }

            String upstreamFqn = upstreamFqnOpt.get();

            // не перетираем уже найденный mapping
            aliasToResolvedFqn.putIfAbsent(aliasKey, upstreamFqn);
            upstreamColumnIndexByTableFqn.putIfAbsent(upstreamFqn, Map.of());
        }

        // 2) Строим общий список columnsLineage
        List<ColumnsLineage> columnsLineage = buildColumnsLineage(
                viewFqn,
                parsed,
                aliasToResolvedFqn,
                upstreamColumnIndexByTableFqn,
                omBaseUrl,
                normalizedPrefixFqn,
                viewSchema,
                idCache
        );

        // 3) Создаём table-level edges, на каждом edge оставляем только относящиеся
        // к конкретной upstream table columnsLineage
        List<AddLineageRequest> out = new ArrayList<>();
        Set<String> emittedUpstreamFqns = new LinkedHashSet<>();

        for (var upstreamRef : parsed.upstreamTables()) {
            Optional<String> upstreamFqnOpt = resolveUpstreamTableFqn(
                    omBaseUrl,
                    normalizedPrefixFqn,
                    viewSchema,
                    upstreamRef,
                    idCache
            );

            if (upstreamFqnOpt.isEmpty()) {
                String debugName = (upstreamRef.schema() == null ? "" : (upstreamRef.schema() + ".")) + upstreamRef.name();
                log.warn("View upstream {} не найден в OMD (schemaView/pg_catalog/information_schema)", debugName);
                continue;
            }

            String upstreamFqn = upstreamFqnOpt.get();

            // дедупликация одного и того же upstream edge
            if (!emittedUpstreamFqns.add(upstreamFqn)) {
                continue;
            }

            List<ColumnsLineage> filtered = columnsLineage.stream()
                    .map(cl -> {
                        List<String> from = Optional.ofNullable(cl.getFromColumns())
                                .orElse(List.of())
                                .stream()
                                .filter(fc -> fc != null && fc.startsWith(upstreamFqn + "."))
                                .distinct()
                                .toList();

                        if (from.isEmpty()) {
                            return null;
                        }

                        return ColumnsLineage.builder()
                                .toColumn(cl.getToColumn())
                                .fromColumns(from)
                                .build();
                    })
                    .filter(Objects::nonNull)
                    .toList();

            String upstreamId = resolveIdCached(omBaseUrl, upstreamFqn, idCache).orElse(null);
            if (upstreamId == null) {
                log.warn("Upstream {} отсутствует в OMD", upstreamFqn);
                continue;
            }

            if (filtered.isEmpty()) {
                log.debug(
                        "View {} -> {}: column-level lineage не найден, создаём table-level edge",
                        upstreamFqn,
                        viewFqn
                );
            }

            out.add(AddLineageRequest.builder()
                    .edge(LineageEdge.builder()
                            .fromEntity(EntityReference.builder().type("table").id(upstreamId).build())
                            .toEntity(EntityReference.builder().type("table").id(viewId).build())
                            .lineageDetails(LineageDetails.builder()
                                    .source(LineageSource.ViewLineage)
                                    .sqlQuery(sql)
                                    .columnsLineage(filtered.isEmpty() ? null : filtered)
                                    .build())
                            .build())
                    .build());
        }

        return out;
    }

    private List<ColumnsLineage> buildColumnsLineage(
            String viewFqn,
            ViewSqlLineageParser.ParsedLineage parsed,
            Map<String, String> aliasToResolvedTableFqn, // alias(lower)->tableFqn
            Map<String, Map<String, String>> upstreamColsByTableFqn, // tableFqn -> (col(lower)->colFqn)
            String omBaseUrl,
            String prefixFqn,
            String viewSchema,
            Map<String, Optional<String>> idCache
    ) {
        List<ColumnsLineage> out = new ArrayList<>();

        for (var mapping : parsed.columnMappings()) {
            String toName = normalizeIdent(mapping.toColumn());
            if (toName == null || toName.isBlank()) {
                continue;
            }

            // Так как metadata view колонок в этом сервисе нет, строим toColumn best-effort
            String toFqn = viewFqn + "." + toName;

            List<String> fromFqns = new ArrayList<>();

            for (var cr : mapping.from()) {
                String holderRaw = normalizeIdent(cr.tableAliasOrName());
                String holderKey = normKey(holderRaw);
                String colName = normalizeIdent(cr.column());

                if (colName == null || colName.isBlank()) {
                    continue;
                }

                String upstreamTableFqn = null;

                if (holderKey != null && !holderKey.isBlank()) {
                    // 1) пробуем как alias/name
                    upstreamTableFqn = aliasToResolvedTableFqn.get(holderKey);

                    // 2) если не нашли — пробуем трактовать как имя таблицы без схемы
                    if (upstreamTableFqn == null) {
                        upstreamTableFqn = resolveUpstreamTableFqn(
                                omBaseUrl,
                                prefixFqn,
                                viewSchema,
                                new ViewSqlLineageParser.TableRef(null, holderRaw),
                                idCache
                        ).orElse(null);
                    }
                } else {
                    // table не указан у колонки
                    // если upstream таблица ровно одна — можем однозначно сопоставить
                    if (parsed.upstreamTables().size() == 1) {
                        var only = parsed.upstreamTables().iterator().next();
                        upstreamTableFqn = resolveUpstreamTableFqn(
                                omBaseUrl,
                                prefixFqn,
                                viewSchema,
                                only,
                                idCache
                        ).orElse(null);
                    }
                }

                if (upstreamTableFqn == null) {
                    continue;
                }

                Map<String, String> upstreamCols = upstreamColsByTableFqn.getOrDefault(upstreamTableFqn, Map.of());

                String fromFqn = upstreamCols.get(normKey(colName));
                if (fromFqn == null) {
                    fromFqn = upstreamTableFqn + "." + colName;
                }

                fromFqns.add(fromFqn);
            }

            if (!fromFqns.isEmpty()) {
                List<String> uniq = new ArrayList<>(new LinkedHashSet<>(fromFqns));

                out.add(ColumnsLineage.builder()
                        .toColumn(toFqn)
                        .fromColumns(uniq)
                        .build());
            }
        }

        return out;
    }

    /**
     * Возвращает FQN upstream таблицы, которая реально существует в OMD.
     * Optional содержит именно table FQN, а не id.
     */
    private Optional<String> resolveUpstreamTableFqn(
            String omBaseUrl,
            String prefixFqn,          // service.db
            String viewSchema,
            ViewSqlLineageParser.TableRef ref,
            Map<String, Optional<String>> idCache
    ) {
        String tableName = normalizeIdent(ref.name());
        String schema = normalizeIdent(ref.schema());

        if (tableName == null || tableName.isBlank()) {
            return Optional.empty();
        }

        // 1) Если schema указана в SQL — используем её
        if (schema != null && !schema.isBlank()) {
            String fqn = String.format("%s.%s.%s", prefixFqn, schema, tableName);
            return resolveIdCached(omBaseUrl, fqn, idCache).isPresent()
                    ? Optional.of(fqn)
                    : Optional.empty();
        }

        // 2) Иначе пробуем: schema view -> pg_catalog -> information_schema
        List<String> candidates = new ArrayList<>(1 + DEFAULT_SCHEMA_FALLBACK.size());
        candidates.add(viewSchema);
        candidates.addAll(DEFAULT_SCHEMA_FALLBACK);

        for (String schemaCandidate : candidates) {
            String normalizedSchema = normalizeIdent(schemaCandidate);
            if (normalizedSchema == null || normalizedSchema.isBlank()) {
                continue;
            }

            String fqn = String.format("%s.%s.%s", prefixFqn, normalizedSchema, tableName);
            if (resolveIdCached(omBaseUrl, fqn, idCache).isPresent()) {
                return Optional.of(fqn);
            }
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

    /**
     * trim + снять двойные кавычки
     */
    private static String normalizeIdent(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Нормализованный ключ для alias/name map
     */
    private static String normKey(String s) {
        String n = normalizeIdent(s);
        return n == null ? null : n.toLowerCase(Locale.ROOT);
    }
}
