package com.gpb.jdata.orda.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.gpb.jdata.orda.repository.TableRepository;
import com.gpb.jdata.orda.util.OrdaUtils;

public class TableMapper {
    public static Map<String, Object> toRequestBody(List<Map<String, Object>> rows,
                                                    List<Map<String, Object>> partitions,
                                                    TableRepository tableRepository) {
        Map<String, Object> first = rows.get(0);

        String schema = (String) first.get("dbschema");
        String table  = (String) first.get("tablename");
        String tableType = (String) first.get("tabletype");

        Map<String, Object> body = new LinkedHashMap<>();

        body.put("databaseSchema", OrdaUtils.fqnSchema(schema));
        body.put("description", first.get("table_description"));
        body.put("displayName", table);
        body.put("name", table);
        body.put("isProjectEntity", false);
        body.put("tableType", tableType);
        body.put("viewDefinition", first.get("view_definition"));

        List<Map<String, Object>> columns = rows.stream()
                .filter(r -> r.get("columnname") != null)
                .map(r -> {
                    String col = (String) r.get("columnname");
                    String dtype = (String) r.get("dtype");
                    Integer dtlen = asInt(r.get("dtlength"));
                    Integer ordinal = asInt(r.get("attnum"));
                    String constraint = null;
                    if (r.get("pk_constraint") != null) {
                        int[] pk = (int[]) r.get("pk_constraint");
                        if (pk != null && ordinal != null && contains(pk, ordinal)) {
                            constraint = "PRIMARY_KEY";
                        }
                    }
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("name", col);
                    c.put("dataType", TypeMapper.mapToOrdaType(dtype));
                    c.put("dataLength", dtlen == null ? 1 : dtlen);
                    c.put("dataTypeDisplay", OrdaUtils.dataTypeDisplay(dtype, dtlen));
                    c.put("description", r.get("column_description"));
                    c.put("fullyQualifiedName", OrdaUtils.fqnColumn(schema, table, col));
                    c.put("constraint", constraint == null ? "NULL" : constraint);
                    c.put("ordinalPosition", ordinal);
                    return c;
                })
                .collect(Collectors.toList());

        body.put("columns", columns);

        List<Map<String, Object>> fkConstraints = rows.stream()
                .filter(r -> r.get("fk_constraint") != null && r.get("foreign_table_id") != null)
                .collect(Collectors.groupingBy(r -> Arrays.toString((int[]) r.get("fk_constraint"))))
                .values().stream()
                .map(group -> {
                    int[] fkCols = (int[]) group.get(0).get("fk_constraint");
                    int foreignTableId = asInt(group.get(0).get("foreign_table_id"));
                    Map<String, Object> refInfo = tableRepository.getTableInfoById(foreignTableId);
                    String refSchema = (String) refInfo.get("schemaname");
                    String refTable  = (String) refInfo.get("tablename");
                    List<String> cols = new ArrayList<>();
                    List<String> refCols = new ArrayList<>();
                    for (Map<String, Object> r : group) {
                        Integer ordinal = asInt(r.get("attnum"));
                        if (ordinal != null && contains(fkCols, ordinal)) {
                            String col = (String) r.get("columnname");
                            cols.add(col);
                            refCols.add(OrdaUtils.fqnColumn(refSchema, refTable, col));
                        }
                    }
                    Map<String, Object> fk = new LinkedHashMap<>();
                    fk.put("constraintType", "FOREIGN_KEY");
                    fk.put("columns", cols);
                    fk.put("referredColumns", refCols);
                    return fk;
                })
                .collect(Collectors.toList());

        body.put("tableConstraints", fkConstraints);

        if (!partitions.isEmpty()) {
            String intervalType = (String) partitions.get(0).get("partition_strategy");
            List<String> partCols = partitions.stream()
                    .map(p -> (String) p.get("attname"))
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());
            Map<String, Object> partition = new LinkedHashMap<>();
            partition.put("columns", partCols);
            partition.put("intervalType", intervalType);
            body.put("tablePartition", partition);

        } else {
            body.put("tablePartition", null);
        }
        return body;
    }
    
    private static Integer asInt(Object o) {
        if (o == null) return null;
        if (o instanceof Integer i) return i;
        if (o instanceof Number n) return n.intValue();
        return null;
    }

    private static boolean contains(int[] arr, int v) {
        if (arr == null) return false;
        for (int x : arr) if (x == v) return true;
        return false;
    }
}
