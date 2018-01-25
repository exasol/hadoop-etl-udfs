package com.exasol.hadoop.hcat;

import com.exasol.json.JsonUtil;
import org.junit.Test;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WebHCatJsonSerializerTest {

    @Test
    public void testSerializeColumnArray() throws Exception {
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("ColumnOne", "int"));
        columns.add(new HCatTableColumn("ColumnTwo", "string"));
        String json = WebHCatJsonSerializer.serializeColumnArray(columns);
        JsonArray actual = JsonUtil.getJsonArray(json);
        String expectedColumns = "[" +
                "{" +
                "   \"name\": \"ColumnOne\"," +
                "   \"type\": \"int\"" +
                "}," +
                "{" +
                "   \"name\": \"ColumnTwo\"," +
                "   \"type\": \"string\"" +
                "}" +
                "]";
        JsonArray expected = JsonUtil.getJsonArray(expectedColumns);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSerializeSerDeParameters() throws Exception {
        List<HCatSerDeParameter> params = new ArrayList<>();
        params.add(new HCatSerDeParameter("param.1", "value.1"));
        params.add(new HCatSerDeParameter("param.2", "value.2"));
        String json = WebHCatJsonSerializer.serializeSerDeParameters(params);
        JsonObject actual = JsonUtil.getJsonObject(json);
        String expectedSerDeProperties = "{" +
                "   \"param.1\": \"value.1\"," +
                "   \"param.2\": \"value.2\"" +
                "}";
        JsonObject expected = JsonUtil.getJsonObject(expectedSerDeProperties);
        assertEquals(expected, actual);
    }

}
