package com.gpb.jdata.utils.writer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.jdata.models.master.PGColumns;
import com.gpb.jdata.models.master.PGViewTable;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ApiRequestCreator {
    static Logger logger = LoggerFactory.getLogger(ApiRequestCreator.class);

    public String schemaCreation(String db, String description, String name, String serviceName) 
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Schema sc = new Schema();

        sc.setDatabase(serviceName + "." + db);
        if (!description.isEmpty() && description != null) {
            sc.setDescription(description);
        }

        sc.setDisplayName(name);
        sc.setName(name);
        String json = mapper.writeValueAsString(sc);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
        return json;
    }

    public String dbCreation(String serviceName, String name, String description) 
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Database db = new Database();
        db.setService(serviceName);
        db.setDisplayName(name);
        db.setName(name);
        db.setDescription(description);
        String json = mapper.writeValueAsString(db);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
        return json;
    }

    public String dbServiceCreation(String name, String type) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        DatabaseService service = new DatabaseService();
        service.setName(name);
        service.setDisplayName(name);
        service.setServiceType(type);
        String json = mapper.writeValueAsString(service);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
        return json;
    }

    public String tableCreation(
            PGViewTable table, 
            String db, 
            String serviceName, 
            List<String> tableConstraints, 
            List<String> primary, 
            List<String> foreign, 
            List<String> check, 
            List<String> unique, 
            List<String> exclude
    ) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("name", table.getId().getTableName());
        node.put("description", table.getTableDescription());

        String fullyQualified = serviceName + "." + db + "." + table.getId().getSchema();
        node.put("databaseSchema",  fullyQualified);
        node = createTableType(table.getTableType(), table.getViewDefinition(), node);

        ArrayNode parentArray = mapper.createArrayNode();
        parentArray = createColumns(table, parentArray, mapper);
        // node.put("columns", parentArray);
        node.set("columns", parentArray);

        ArrayNode constraintsArray = mapper.createArrayNode();
        constraintsArray = createConstraints(mapper, constraintsArray, table, primary, 
                foreign, check, unique, exclude, fullyQualified);
        if (!tableConstraints.isEmpty() && tableConstraints != null) {
            // node.put("tableConstraints", constraintsArray);
            node.set("tableConstraints", constraintsArray);
        }

        String json = mapper.writeValueAsString(node);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
        return json;
    }
    private ArrayNode createConstraints(
            ObjectMapper mapper, 
            ArrayNode constraintsArray, 
            PGViewTable table, 
            List<String> primary,
            List<String> foreign, 
            List<String> check, 
            List<String> unique, 
            List<String> exclude, 
            String fullyQualified) {
        ArrayNode constraintpColumnsArray = mapper.createArrayNode();
        ArrayNode constraintfColumnsArray = mapper.createArrayNode();
        ArrayNode constraintuColumnsArray = mapper.createArrayNode();
        ObjectNode constraintpTypeNode = mapper.createObjectNode();
        ObjectNode constraintfTypeNode = mapper.createObjectNode();
        ObjectNode constraintuTypeNode = mapper.createObjectNode();
        ArrayNode referencedColumnsArray = mapper.createArrayNode();

        if (!primary.isEmpty() && primary != null) {
            constraintpTypeNode.put("constraintType", "PRIMARY_KEY");
        }

        if (!foreign.isEmpty() && foreign != null) {
            constraintfTypeNode.put("constraintType", "FOREIGN_KEY");
        }

        if (!unique.isEmpty() && unique != null) {
            constraintuTypeNode.put("constraintType", "UNIQUE");
        }

        List<PGColumns> columns = table.getColumns();
        for (PGColumns c : columns) {
            String column = c.getName();
            if (primary != null && !primary.isEmpty()) {
                for (String p : primary) {
                    if (p.contains(column)) {
                        constraintpColumnsArray.add(column);
                    }
                }
            }
            if (foreign != null && !foreign.isEmpty()) {
                for (String f : foreign) {
                    if (f.contains(column)) {
                        constraintfColumnsArray.add(column);
                        String tableName = f.substring(f.indexOf("REFERENCES ") + 11, f.lastIndexOf("("));
                        String col = f.substring(f.lastIndexOf("(") + 1, f.lastIndexOf(")"));
                        String full = fullyQualified + "." + tableName + "." + col;
                        referencedColumnsArray.add(full);
                    }
                }
            }
            if (unique != null && !unique.isEmpty()) {
                for (String u : unique) {
                    if (u.contains(column)) {
                        constraintuColumnsArray.add(column);
                    }
                }
            }
        }

        if (primary != null && !primary.isEmpty()) {
            constraintpTypeNode.set("columns", constraintpColumnsArray);
            constraintsArray.add(constraintpTypeNode);
        }

        if (unique != null && !unique.isEmpty()) {
            constraintuTypeNode.set("columns", constraintuColumnsArray);
            constraintsArray.add(constraintuTypeNode);
        }

        if (foreign != null && !foreign.isEmpty()) {
            constraintfTypeNode.set("columns", constraintfColumnsArray);
            constraintfTypeNode.set("referredColumns", referencedColumnsArray);
            constraintsArray.add(constraintfTypeNode);
        }
        return constraintsArray;
    }

    private ArrayNode createColumns(PGViewTable table, ArrayNode parentArray, ObjectMapper mapper) {
        List<PGColumns> columns = table.getColumns();
        for (PGColumns c : columns) {
            ObjectNode columnNode = mapper.createObjectNode();
            columnNode.put("name", c.getName());
            columnNode.put("dataType", c.getDataType());
            columnNode.put("dataTypeDisplay", c.getDataTypeDisplay());
            if (c.getDataTypeLength() != 0) {
                columnNode.put("dataLength", c.getDataTypeLength());
            }
            columnNode.put("constraint", c.getConstraint());
            columnNode.put("description", c.getDescription());
            parentArray.add(columnNode);
        }
        return parentArray;
    }

    private ObjectNode createTableType(String tableType, String viewDef, ObjectNode node) {
        if (tableType.contains("r")) {
            node.put("tableType", "Regular");
        } else if (tableType.contains("i")) {
            node.put("tableType", "Index");
        } else if (tableType.contains("s")) {
            node.put("tableType", "Sequence");
        } else if (tableType.contains("v")) {
            node.put("tableType", "View");
            node.put("viewDefinition", viewDef);
        } else if (tableType.contains("m")) {
            node.put("tableType", "Materialized View");
        } else if (tableType.contains("c")) {
            node.put("tableType", "Composite");
        } else if (tableType.contains("t")) {
            node.put("tableType", "Toast");
        } else if (tableType.contains("f")) {
            node.put("tableType", "Foreign");
        }
        return node;
    }

    public String deleteTable(String relname) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("fullyQualifiedName", relname);
        String json = mapper.writeValueAsString(node);
        return json;
    }
}
