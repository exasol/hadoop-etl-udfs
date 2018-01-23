package com.exasol.hadoop.parquet;

public class ExaParquetTypeInfo {
    String name;
    String typeRepitition;
    String primitiveTypeName;
    String originalType;
    int length;

    public ExaParquetTypeInfo(String name, String typeRepitition, String primitiveTypeName) {
        this(name, typeRepitition, primitiveTypeName, null, 0);
    }

    public ExaParquetTypeInfo(String name, String typeRepitition, String primitiveTypeName, String originalType) {
        this(name, typeRepitition, primitiveTypeName, originalType, 0);
    }

    public ExaParquetTypeInfo(String name, String typeRepitition, String primitiveTypeName, String originalType, int length) {
        this.name = name;
        this.typeRepitition = typeRepitition;
        this.primitiveTypeName = primitiveTypeName;
        this.originalType = originalType;
        this.length = length;
    }
}
