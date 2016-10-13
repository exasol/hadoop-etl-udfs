package com.exasol.json;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import javax.json.stream.JsonGenerator;

public class JsonUtil {

    public static JsonObject getJsonObject(String data) throws Exception {
        JsonReader jr = Json.createReader(new StringReader(data));
        JsonObject obj = jr.readObject();
        jr.close();
        return obj;
    }

    public static JsonArray getJsonArray(String data) throws Exception {
        JsonReader jr = Json.createReader(new StringReader(data));
        JsonArray arr = jr.readArray();
        jr.close();
        return arr;
    }
    
    public static String prettyJson(JsonObject obj) {
        Map<String, Boolean> config = new HashMap<>();
        config.put(JsonGenerator.PRETTY_PRINTING, true);
        StringWriter strWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(strWriter);
        JsonWriter jsonWriter = Json.createWriterFactory(config).createWriter(pw);
        jsonWriter.writeObject(obj);
        return strWriter.toString();
    }

}
