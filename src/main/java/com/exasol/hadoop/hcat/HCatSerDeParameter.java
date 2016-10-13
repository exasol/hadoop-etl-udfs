package com.exasol.hadoop.hcat;


public class HCatSerDeParameter {
    
    private String name;
    private String value;
    
    public HCatSerDeParameter(String name, String value) {
        this.name = name;
        this.value = value;
    }
    
    @Override
    public String toString() {
        return name + ": " + value;
    }
    
    public String getName() {
        return name;
    }
    
    public String getValue() {
        return value;
    }
}
