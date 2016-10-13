package com.exasol.hadoop.hcat;

import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;

public class WebHCatJsonSerializer {
    
    public static String serializeColumnArray(List<HCatTableColumn> columns)
    {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (HCatTableColumn column : columns) {
            arrayBuilder.add(Json.createObjectBuilder().add("name", column.getName()).add("type", column.getDataType()));
        }
        return arrayBuilder.build().toString();
    }
    
    public static String serializeSerDeParameters(List<HCatSerDeParameter> serDeParameters) {
        JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
        for (HCatSerDeParameter param : serDeParameters) {
            objectBuilder.add(param.getName(), param.getValue());
        }
        return objectBuilder.build().toString();
    }
    
}
