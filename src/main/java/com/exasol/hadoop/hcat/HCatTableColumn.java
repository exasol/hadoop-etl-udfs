package com.exasol.hadoop.hcat;

public class HCatTableColumn {

    private String name;
    private String dataType;
    
    public HCatTableColumn(String name, String dataType) {
        this.name = name;
        this.dataType = dataType;
    }
    
    @Override
    public String toString() {
        return name + ": " + dataType;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDataType() {
        return dataType;
    }

}
