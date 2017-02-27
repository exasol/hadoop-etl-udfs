package com.exasol.adapter;

/**
 * Created by np on 2/7/2017.
 */
public class SqlGenerationContext {

    private String catalogName;
    private String schemaName;
    private boolean isLocal;

    public SqlGenerationContext(String catalogName, String schemaName, boolean isLocal) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.isLocal = isLocal;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean isLocal() {
        return isLocal;
    }
}
