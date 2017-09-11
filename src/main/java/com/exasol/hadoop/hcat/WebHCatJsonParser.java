package com.exasol.hadoop.hcat;

import com.exasol.json.JsonUtil;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class WebHCatJsonParser {
    
    public static HCatTableMetadata parse(String json) throws Exception
    {
        JsonObject response = JsonUtil.getJsonObject(json);
        System.out.println("Parse JSON Response: " + JsonUtil.prettyJson(response));
        
        String location = response.getString("location");   // e.g. "hdfs://vm031.cos.dev.exasol.com:8020/user/hive/warehouse/albums_rc_multi_part"
        List<HCatTableColumn> columns = parseColumnArray(response.getJsonArray("columns"));
        List<HCatTableColumn> partitionColumns = parseColumnArray(response.getJsonArray("partitionColumns"));
        String tableType = response.getString("tableType");
        String inputFormat = response.getString("inputFormat");
        String outputFormat = response.getString("outputFormat");
        String serDe = response.getJsonObject("sd").getJsonObject("serdeInfo").getString("serializationLib");
        List<HCatSerDeParameter> serDeParameters = parseSerDeParameters(response.getJsonObject("sd").getJsonObject("serdeInfo").getJsonObject("parameters"));

        return new HCatTableMetadata(location, columns, partitionColumns, tableType, inputFormat, outputFormat, serDe, serDeParameters);
    }
    
    private static List<HCatTableColumn> parseColumnArray(JsonArray array) {
        List<HCatTableColumn> columns = new ArrayList<HCatTableColumn>();
        if (array != null && !array.isEmpty()) {
            for (JsonValue column : array) {
                JsonObject col = (JsonObject) column;
                columns.add(new HCatTableColumn(col.getString("name"), col.getString("type")));
            }
        }
        return columns;
    }
    
    public static List<HCatTableColumn> parseColumnArray(String json) throws Exception {
        List<HCatTableColumn> columns;
        if (json != null && !json.isEmpty()) {
            JsonArray arr = JsonUtil.getJsonArray(json);
            columns = WebHCatJsonParser.parseColumnArray(arr);
        } else {
            columns = new ArrayList<>();
        }
        return columns;
    }
    
    public static List<HCatSerDeParameter> parseSerDeParameters(JsonObject obj) {
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        for (Entry<String, JsonValue> serdeParam : obj.entrySet()) {
            assert(serdeParam.getValue().getValueType() == ValueType.STRING);
            serDeParameters.add(new HCatSerDeParameter(serdeParam.getKey(), ((JsonString)serdeParam.getValue()).getString()));
        }
        return serDeParameters;
    }
    
    public static List<HCatSerDeParameter> parseSerDeParameters(String json) throws Exception {
        assert(!(json == null) && !json.isEmpty());
        JsonObject obj = JsonUtil.getJsonObject(json);
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        for (Entry<String, JsonValue> param : obj.entrySet()) {
            assert(param.getValue().getValueType() == ValueType.STRING);
            serDeParameters.add(new HCatSerDeParameter(param.getKey(), ((JsonString)param.getValue()).getString()));
        }
        return serDeParameters;
    }

}
