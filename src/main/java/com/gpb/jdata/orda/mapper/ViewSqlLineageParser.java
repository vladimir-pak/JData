package com.gpb.jdata.orda.mapper;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.LinkedHashSet;
import java.util.Set;

public final class ViewSqlLineageParser {

	public record TableRef(String schema, String name) {}

	public Set<TableRef> extractUpstreamTables(String viewDefinition) {
		if (viewDefinition == null || viewDefinition.isBlank()) return Set.of();

		try {
			Statement stmt = CCJSqlParserUtil.parse(viewDefinition);

			// TablesNamesFinder возвращает имена таблиц как строки (с учетом schema при наличии)
			TablesNamesFinder finder = new TablesNamesFinder();
			Set<String> tableNames = new LinkedHashSet<>(finder.getTableList(stmt));

			Set<TableRef> out = new LinkedHashSet<>();
			for (String qname : tableNames) {
				// qname может быть: schema.table или просто table
				String schema = null;
				String name = qname;

				int dot = qname.indexOf('.');
				if (dot > 0 && dot < qname.length() - 1) {
					schema = qname.substring(0, dot);
					name = qname.substring(dot + 1);
				}
				out.add(new TableRef(schema, name));
			}
			return out;

		} catch (Exception e) {
			return Set.of();
		}
	}
}
