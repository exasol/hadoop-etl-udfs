package com.exasol.jsonpath;

import com.exasol.hadoop.hcat.HCatTableColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutputColumnSpecUtil {

    /**
     * @param outputSpecification A comma separated list of output columns and optional JsonPath suffixes. The whole path is case sensitive.
     */
    public static List<OutputColumnSpec> parseOutputSpecification(String outputSpecification, List<HCatTableColumn> columns, List<HCatTableColumn> partitionColumns) {
        List<OutputColumnSpec> outputColumns = new ArrayList<>();
        try {
            String[] elements = outputSpecification.split(",");
            for (int i=0; i<elements.length; ++i) {
                List<JsonPathElement> path = JsonPathParser.parseJsonPath(elements[i].trim());
                // The first element must contain the column name
                if (path.get(0).getType() != JsonPathElement.Type.FIELD) {
                    throw new IllegalArgumentException("Could not parse output column specification. Invalid element " + i + ": " + elements[i]);
                }
                String columnName = ((JsonPathFieldElement)path.get(0)).getFieldName();
                int columnPosition = findColumnPositionByName(columnName, columns, partitionColumns);
                if (path.size() == 1) {
                    // Path only consisted of column name
                    outputColumns.add(new OutputColumnSpec(i, columnName, columnPosition));
                } else {
                    // Path consists of column name and jsonpath suffix, e.g. "column.field1[0]"
                    path.remove(0);
                    outputColumns.add(new OutputColumnSpec(i, columnName, columnPosition, path));
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse output column specification due to error '" + e.toString() + "' for output specification: " + outputSpecification, e);
        }
        return outputColumns;
    }
    
    public static List<OutputColumnSpec> parseOutputSpecification(String outputSpecification, List<HCatTableColumn> columns) {
        return parseOutputSpecification(outputSpecification, columns, new ArrayList<HCatTableColumn>());
    }
    
    private static int findColumnPositionByName(String columnName, List<HCatTableColumn> columns, List<HCatTableColumn> partitions) {
        for (int i=0; i<columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        for (int i=0; i<partitions.size(); i++) {
            if (partitions.get(i).getName().equals(columnName)) {
                return i + columns.size();
            }
        }
        throw new RuntimeException("Could not find column " + columnName + ". Note that the columns are case-sensitive");
    }

    /**
     * Checks whether the left path is prefic of the right path. Must be a real prefix.
     * 
     * Example: Return true if left path is 'field1[0]', and the right path is 'field1[0].field2. 
     */
    public static boolean leftIsPrefixOfRightPath(List<JsonPathElement> leftPath, List<JsonPathElement> rightPath) {
        if (leftPath.size() >= rightPath.size()) {
            return false;
        }
        boolean isPrefixOfOtherPath = true;
        for (int i=0; i<leftPath.size(); i++) {
            if (!leftPath.get(i).equals(rightPath.get(i))) {
                isPrefixOfOtherPath = false;
            }
        }
        return isPrefixOfOtherPath;
    }

    public static Map<Integer, List<OutputColumnSpec>> getOutputSpecsForColumns(List<OutputColumnSpec> outputColumns) {
        Map<Integer, List<OutputColumnSpec>> map = new HashMap<>();
        for (OutputColumnSpec outCol : outputColumns) {
            int colPos = outCol.getColumnPosition();
            if (map.containsKey(colPos)) {
                map.get(colPos).add(outCol);
            } else {
                List<OutputColumnSpec> list = new ArrayList<>();
                list.add(outCol);
                map.put(colPos, list);
            }
        }
        return map;
    }

    public static List<OutputColumnSpec> generateDefaultOutputSpecification(
    		List<HCatTableColumn> columns,
    		List<HCatTableColumn> partitionColumns) {
    	List<OutputColumnSpec> columnsSpec = new ArrayList<>();
    	for (int i=0; i<columns.size(); i++) {
    		columnsSpec.add(new OutputColumnSpec(i, columns.get(i).getName(), i));
    	}
    	for (int i=0; i<partitionColumns.size(); i++) {
    		columnsSpec.add(new OutputColumnSpec(i + columns.size(), partitionColumns.get(i).getName(), i + columns.size()));
    	}
    	return columnsSpec;
    }
}
