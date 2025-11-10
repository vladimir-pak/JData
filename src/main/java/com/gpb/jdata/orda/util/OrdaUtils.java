package com.gpb.jdata.orda.util;

public class OrdaUtils {
    private static final String ORG = "NPD_GP";
    private static final String DB  = "adb";

    public static String fqnSchema(String schema) {
        return ORG + "." + DB + "." + schema;
    }

    public static String fqnTable(String schema, String table) {
        return fqnSchema(schema) + "." + table;
    }

    public static String fqnColumn(String schema, String table, String column) {
        return fqnTable(schema, table) + "." + column;
    }
    
    public static String dataTypeDisplay(String pgTypName, Integer atttypmod) {
        String base = (pgTypName == null ? "" : pgTypName).toLowerCase();
        if (atttypmod != null && atttypmod > 0) {
            return base + "(" + atttypmod + ")";
        }
        return base;
    }
}
