package com.exasol.adapter;

import com.exasol.utils.JsonHelper;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class ColumnAdapterNotes {

    private String originalTypeName;
    private boolean partitionedColumn;

    public ColumnAdapterNotes(String originalTypeName,boolean partitionedColumn) {
        this.originalTypeName = originalTypeName;
        this.partitionedColumn = partitionedColumn;
    }


    public static String serialize(ColumnAdapterNotes notes) {
        JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        JsonObjectBuilder builder = factory.createObjectBuilder()
                .add("originalTypeName", notes.getOriginalTypeName())
                .add("partitionedColumn", notes.isPartitionedColumn());
        return builder.build().toString();
    }


    public String getOriginalTypeName() {
        return originalTypeName;
    }
    public boolean isPartitionedColumn() {
        return partitionedColumn;
    }


    public static ColumnAdapterNotes deserialize(String columnAdapterNotes, String columnName) {
        if (columnAdapterNotes == null || columnAdapterNotes.isEmpty()) {
            throw new RuntimeException(getException(columnName));
        }
        JsonObject root;
        try {
            root = JsonHelper.getJsonObject(columnAdapterNotes);
        } catch (Exception ex) {
            throw new RuntimeException(getException(columnName));
        }
        checkKey(root, "originalTypeName", columnName);
        checkKey(root, "partitionedColumn", columnName);
        return new ColumnAdapterNotes(
                root.getString("originalTypeName"),
                root.getBoolean("partitionedColumn")
        );
    }

    private static void checkKey(JsonObject root, String key, String columnName) {
        if (!root.containsKey(key)) {
            throw new RuntimeException(getException(columnName));
        }
    }

    private static String getException(String columnName) {
        return "The adapternotes field of column " + columnName + " could not be parsed. Please refresh the virtual schema.";
    }
}
