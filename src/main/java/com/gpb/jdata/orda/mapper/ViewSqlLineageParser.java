package com.gpb.jdata.orda.mapper;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;

import org.springframework.stereotype.Component;

import java.util.*;

@Component
public final class ViewSqlLineageParser {

    public record TableRef(String schema, String name) {}

    public record ColumnRef(String tableAliasOrName, String column) {}

    public record ColumnMapping(String toColumn, Set<ColumnRef> from) {}

    public record ParsedLineage(
            Map<String, TableRef> aliasToTable,   // alias/name -> physical table
            Set<TableRef> upstreamTables,         // all physical upstream tables
            List<ColumnMapping> columnMappings    // output column -> base input columns
    ) {}

    /**
     * Источники текущего scope:
     * 1) обычные таблицы alias/name -> physical table
     * 2) derived table alias -> (derived col -> base columns)
     */
    private record SourceDescriptor(
            Map<String, TableRef> aliasToTable,
            Map<String, Map<String, Set<ColumnRef>>> derivedAliasToColumns
    ) {}

    public ParsedLineage parse(String viewDefinition) {
        if (viewDefinition == null || viewDefinition.isBlank()) {
            return empty();
        }

        try {
            Statement stmt = CCJSqlParserUtil.parse(viewDefinition);

            // В JSqlParser 5.x SelectBody удалён;
            // PlainSelect сам является Select-узлом.
            if (stmt instanceof PlainSelect ps) {
                return parsePlainSelect(ps);
            }

            // Иногда верхний уровень может быть ParenthesedSelect или SetOperationList.
            if (stmt instanceof ParenthesedSelect parenthesedSelect) {
                Select inner = parenthesedSelect.getSelect();
                return parseSelect(inner);
            }

            if (stmt instanceof SetOperationList setOperationList) {
                return parseSetOperationList(setOperationList);
            }

            if (stmt instanceof Select select) {
                return parseSelect(select);
            }

            return empty();
        } catch (Exception e) {
            return empty();
        }
    }

    private ParsedLineage parseSelect(Select select) {
        if (select == null) {
            return empty();
        }

        if (select instanceof PlainSelect ps) {
            return parsePlainSelect(ps);
        }

        if (select instanceof ParenthesedSelect psel) {
            return parseSelect(psel.getSelect());
        }

        if (select instanceof SetOperationList setOp) {
            return parseSetOperationList(setOp);
        }

        // Values и прочие экзотические варианты сейчас не поддерживаем.
        return empty();
    }

    /**
     * Для UNION / INTERSECT / EXCEPT:
     * - собираем upstream физические таблицы из всех веток
     * - column lineage склеиваем best-effort по всем PlainSelect веткам
     *
     * Это не идеально для сложных UNION-кейсов, но безопаснее, чем просто потерять lineage.
     */
    private ParsedLineage parseSetOperationList(SetOperationList setOp) {
        if (setOp == null || setOp.getSelects() == null || setOp.getSelects().isEmpty()) {
            return empty();
        }

        Map<String, TableRef> aliasToTable = new LinkedHashMap<>();
        Set<TableRef> upstreamTables = new LinkedHashSet<>();
        List<ColumnMapping> columnMappings = new ArrayList<>();

        for (Select part : setOp.getSelects()) {
            ParsedLineage parsed = parseSelect(part);
            aliasToTable.putAll(parsed.aliasToTable());
            upstreamTables.addAll(parsed.upstreamTables());
            columnMappings.addAll(parsed.columnMappings());
        }

        return new ParsedLineage(aliasToTable, upstreamTables, columnMappings);
    }

    private ParsedLineage parsePlainSelect(PlainSelect ps) {
        SourceDescriptor sources = extractSources(ps);

        Map<String, TableRef> aliasToTable = sources.aliasToTable();
        Set<TableRef> upstreamTables = new LinkedHashSet<>(aliasToTable.values());

        List<ColumnMapping> columnMappings = extractColumnMappings(ps, sources);

        return new ParsedLineage(aliasToTable, upstreamTables, columnMappings);
    }

    private SourceDescriptor extractSources(PlainSelect ps) {
        Map<String, TableRef> aliasToTable = new LinkedHashMap<>();
        Map<String, Map<String, Set<ColumnRef>>> derived = new LinkedHashMap<>();

        addFromItem(ps.getFromItem(), aliasToTable, derived);

        List<Join> joins = ps.getJoins();
        if (joins != null) {
            for (Join join : joins) {
                addFromItem(join.getRightItem(), aliasToTable, derived);
            }
        }

        return new SourceDescriptor(aliasToTable, derived);
    }

    private void addFromItem(
            FromItem fi,
            Map<String, TableRef> aliasToTable,
            Map<String, Map<String, Set<ColumnRef>>> derived
    ) {
        if (fi == null) {
            return;
        }

        // 1) Обычная физическая таблица
        if (fi instanceof Table table) {
            registerTable(table, aliasToTable);
            return;
        }

        // 2) Подзапрос в FROM: FROM (select ...) t
        if (fi instanceof ParenthesedSelect psel) {
            handleParenthesedSelect(psel, aliasToTable, derived);
            return;
        }

        // 3) Скобочная join-конструкция:
        // FROM a JOIN (b LEFT JOIN c ON ...) x ON ...
        if (fi instanceof ParenthesedFromItem pfi) {
            addFromItem(pfi.getFromItem(), aliasToTable, derived);

            List<Join> joins = pfi.getJoins();
            if (joins != null) {
                for (Join join : joins) {
                    addFromItem(join.getRightItem(), aliasToTable, derived);
                }
            }
        }
    }

    private void registerTable(Table table, Map<String, TableRef> aliasToTable) {
        String schema = normalizeIdent(table.getSchemaName());
        String name = normalizeIdent(table.getName());
        String alias = table.getAlias() != null ? normalizeIdent(table.getAlias().getName()) : null;

        if (name == null || name.isBlank()) {
            return;
        }

        TableRef ref = new TableRef(schema, name);

        if (alias != null && !alias.isBlank()) {
            aliasToTable.put(alias, ref);
            aliasToTable.put(alias.toLowerCase(Locale.ROOT), ref);
        }

        aliasToTable.put(name, ref);
        aliasToTable.put(name.toLowerCase(Locale.ROOT), ref);
    }

    private void handleParenthesedSelect(
            ParenthesedSelect psel,
            Map<String, TableRef> aliasToTable,
            Map<String, Map<String, Set<ColumnRef>>> derived
    ) {
        String alias = psel.getAlias() != null ? normalizeIdent(psel.getAlias().getName()) : null;
        Select inner = psel.getSelect();

        ParsedLineage innerParsed = parseSelect(inner);
        if (innerParsed.upstreamTables().isEmpty()
                && innerParsed.aliasToTable().isEmpty()
                && innerParsed.columnMappings().isEmpty()) {
            return;
        }

        // Физические таблицы внутреннего scope пробрасываем наверх,
        // чтобы upstreamTables не терялись.
        aliasToTable.putAll(innerParsed.aliasToTable());

        // Если у подзапроса есть alias, то строим derived alias map:
        // t.id -> [u.id], t.name -> [u.name], ...
        if (alias != null && !alias.isBlank()) {
            Map<String, Set<ColumnRef>> derivedCols = new LinkedHashMap<>();

            for (ColumnMapping cm : innerParsed.columnMappings()) {
                String col = normalizeIdent(cm.toColumn());
                if (col == null || col.isBlank()) {
                    continue;
                }

                derivedCols.put(col.toLowerCase(Locale.ROOT), new LinkedHashSet<>(cm.from()));
            }

            if (!derivedCols.isEmpty()) {
                derived.put(alias.toLowerCase(Locale.ROOT), derivedCols);
                derived.put(alias, derivedCols);
            }
        }
    }

    private List<ColumnMapping> extractColumnMappings(PlainSelect ps, SourceDescriptor sources) {
        List<ColumnMapping> out = new ArrayList<>();
        List<SelectItem<?>> items = ps.getSelectItems();
        if (items == null || items.isEmpty()) {
            return out;
        }

        for (SelectItem<?> item : items) {
            Expression expr = item.getExpression();

            // * и t.* пока пропускаем
            if (expr instanceof AllColumns || expr instanceof AllTableColumns) {
                continue;
            }

            String toCol = item.getAlias() != null ? normalizeIdent(item.getAlias().getName()) : null;

            // Если alias нет, а это простая колонка — берём имя колонки
            if ((toCol == null || toCol.isBlank()) && expr instanceof Column c) {
                toCol = normalizeIdent(c.getColumnName());
            }

            // Сложное выражение без alias безопасно не именуем
            if (toCol == null || toCol.isBlank()) {
                continue;
            }

            Set<ColumnRef> fromCols = new LinkedHashSet<>();
            collectResolvedColumnRefs(expr, fromCols, sources);

            if (!fromCols.isEmpty()) {
                out.add(new ColumnMapping(toCol, fromCols));
            }
        }

        return out;
    }

    private void collectResolvedColumnRefs(
            Expression expr,
            Set<ColumnRef> out,
            SourceDescriptor sources
    ) {
        if (expr == null) {
            return;
        }

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                String holder = column.getTable() != null
                        ? normalizeIdent(column.getTable().getName())
                        : null;

                String holderNorm = normKey(holder);
                String col = normalizeIdent(column.getColumnName());

                if (col == null || col.isBlank()) {
                    return;
                }

                // 1) t.id -> раскрываем в базовые колонки derived table
                if (holderNorm != null) {
                    Map<String, Set<ColumnRef>> derivedCols =
                            sources.derivedAliasToColumns().get(holderNorm);

                    if (derivedCols != null) {
                        Set<ColumnRef> base = derivedCols.get(col.toLowerCase(Locale.ROOT));
                        if (base != null && !base.isEmpty()) {
                            out.addAll(base);
                            return;
                        }
                    }
                }

                // 2) Обычная колонка физической таблицы / alias
                out.add(new ColumnRef(holder, col));
            }
        });
    }

    private ParsedLineage empty() {
        return new ParsedLineage(Map.of(), Set.of(), List.of());
    }

    private static String normalizeIdent(String s) {
        if (s == null) {
            return null;
        }

        String v = s.trim();
        if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            v = v.substring(1, v.length() - 1);
        }
        return v;
    }

    private static String normKey(String s) {
        String n = normalizeIdent(s);
        return n == null ? null : n.toLowerCase(Locale.ROOT);
    }
}
