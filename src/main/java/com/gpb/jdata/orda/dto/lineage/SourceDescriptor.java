package com.gpb.jdata.orda.dto.lineage;

import java.util.Map;
import java.util.Set;

import com.gpb.jdata.orda.mapper.ViewSqlLineageParser.ColumnRef;
import com.gpb.jdata.orda.mapper.ViewSqlLineageParser.TableRef;

public record SourceDescriptor (
        Map<String, TableRef> aliasToTable,
        Map<String, Map<String, Set<ColumnRef>>> derivedAliasToColumns
) {}
