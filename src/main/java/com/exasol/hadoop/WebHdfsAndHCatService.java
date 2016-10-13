package com.exasol.hadoop;

import org.apache.commons.io.IOUtils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper Methods to interact with webHDFS and webHCatalog
 */
public class WebHdfsAndHCatService {
    public enum Service {
        HDFS,
        HCAT
    }

    private static Map<String, String> hiveJavaTypes = null;

    static {
        Map<String, String> types = new HashMap<>();
        types.put("tinyint", "java.lang.Byte");
        types.put("smallint", "java.lang.Short");
        types.put("int", "java.lang.Integer");
        types.put("bigint", "java.lang.Long");
        types.put("float", "java.lang.Float");
        types.put("double", "java.lang.Double");
        types.put("decimal", "java.math.BigDecimal");
        types.put("timestamp", "java.sql.Timestamp");
        types.put("date", "java.sql.Date");
        types.put("string", "java.lang.String");
        types.put("varchar", "java.lang.String");
        types.put("char", "java.lang.String");
        types.put("boolean", "java.lang.Boolean");
        types.put("binary", "java.lang.String");
        types.put("array", "java.lang.String");
        types.put("map", "java.lang.String");
        types.put("struct", "java.lang.String");
        types.put("uniontype", "java.lang.String");
        hiveJavaTypes = Collections.unmodifiableMap(types);
    }

    public static String getJavaType(String hiveType) throws Exception {
        return hiveJavaTypes.get(hiveType);
    }

    public static String getExtendedTableInfo(String webHCatServer, String dbName, String tableName, String hdfsAndHCatUser) throws Exception {
        String requestPath = "ddl/database/" + dbName + "/table/" + tableName;
        String requestQuery = "format=extended";
        System.out.println("Retrieving table infos now. Request: " + requestPath + " query: " + requestQuery);
        return get(WebHdfsAndHCatService.Service.HCAT, webHCatServer, requestPath, requestQuery, hdfsAndHCatUser);
    }

    private static String buildUrl(Service service, String serverAndPort, String path, String query, String user) {
        String urlStr = "";
        switch (service) {
        case HDFS:
            urlStr = "http://" + serverAndPort + "/webhdfs/v1/";
            break;
        case HCAT:
            urlStr = "http://" + serverAndPort + "/templeton/v1/";
            break;
        default:
            throw new RuntimeException("Unexpected service " + service);
        }
        if (path != null) {
            urlStr += path;
        }
        urlStr += "?user.name=" + user;
        if (query != null) {
            urlStr += "&" + query;
        }
        return urlStr;
    }

    public static String get(Service service, String serverAndPort, String path, String query, String user)
            throws Exception {
        HttpURLConnection httpConn = null;
        String urlStr = buildUrl(service, serverAndPort, path, query, user);
        try {
            URL url = new URL(urlStr);
            httpConn = (HttpURLConnection) url.openConnection();
            httpConn.setRequestMethod("GET");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setConnectTimeout(60000);
            httpConn.setReadTimeout(60000);
            HttpURLConnection.setFollowRedirects(true);
            int respCode = httpConn.getResponseCode();
            if (respCode != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Unexpected return code " + respCode);
            }
            return IOUtils.toString(httpConn.getInputStream(), "UTF-8");
        }catch (Exception e) {
            throw new RuntimeException("Problem accessing web" + service.toString() + " (" + urlStr + "): " + e.getMessage(), e);
        } finally {
            if (httpConn != null) {
                httpConn.disconnect();
            }
        }
    }
}
