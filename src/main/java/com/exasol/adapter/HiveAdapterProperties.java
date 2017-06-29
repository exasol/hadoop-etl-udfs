package com.exasol.adapter;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;

import java.util.*;

public class HiveAdapterProperties {

    static final String PROP_SCHEMA_NAME = "HCAT_DB";
    static final String PROP_TABLE_NAME = "HCAT_TABLE";
    static final String PROP_CONNECTION_ADDRESS = "HCAT_ADDRESS";
    static final String PROP_USERNAME = "HDFS_USER";
    static final String PROP_CONNECTION_NAME = "CONNECTION_NAME";
    static final String PROP_TABLES = "TABLE_FILTER";
    static final String PROP_HDFS_URL = "HDFS_URL";



    private static String getProperty(Map<String, String> properties, String name, String defaultValue) {
        if (properties.containsKey(name)) {
            return properties.get(name);
        } else {
            return defaultValue;
        }
    }

    public static String getSchema(Map<String, String> properties) {
        return getProperty(properties, PROP_SCHEMA_NAME, "");
    }

    public static String getHdfsUrls(Map<String, String> properties){
        return getProperty(properties, PROP_HDFS_URL, "");
    }

    public static String getTableName(Map<String, String> properties) {
        return getProperty(properties, PROP_TABLE_NAME, "");
    }

    public static boolean userSpecifiedConnection(Map<String, String> properties) {
        String connName = getProperty(properties, PROP_CONNECTION_NAME, "");
        return (connName != null && !connName.isEmpty());
    }

    public static String getConnectionName(Map<String, String> properties) {
        String connName = getProperty(properties, PROP_CONNECTION_NAME, "");
        assert(connName != null && !connName.isEmpty());
        return connName;
    }

    public static String getUsername(Map<String, String> properties){
        return getProperty(properties, PROP_CONNECTION_NAME, "");
    }

    /**
     * Returns the credentials for the remote system. These are either directly specified
     * in the properties or obtained from a connection (requires privilege to access the connection
     * .
     */
    public static ExaConnectionInformation getConnectionInformation(Map<String, String> properties, ExaMetadata exaMeta) {
        String connName = getProperty(properties, PROP_CONNECTION_NAME, "");
        if (connName != null && !connName.isEmpty()) {
            try {
                ExaConnectionInformation connInfo = exaMeta.getConnection(connName);
                String connectionAddress = properties.get(PROP_CONNECTION_ADDRESS);
                return new ExaConnectionInformationHdfs(connectionAddress, connInfo.getUser(), connInfo.getPassword());
            } catch (ExaConnectionAccessException e) {
                throw new RuntimeException("Could not access the connection information of connection " + connName + ". Error: " + e.toString());
            }
        } else {
            String connectionAddress = properties.get(PROP_CONNECTION_ADDRESS);
            String user = properties.get(PROP_USERNAME);
            return new ExaConnectionInformationHdfs(connectionAddress, user, null);
        }
    }

    public static class ExaConnectionInformationHdfs implements ExaConnectionInformation {

        private String address;
        private String user;        // can be null
        private String password;    // can be null


        public ExaConnectionInformationHdfs(String address, String user, String password) {
            this.address = address;
            this.user = user;
            this.password = password;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.PASSWORD;
        }

        @Override
        public String getAddress() {
            return this.address;
        }

        @Override
        public String getUser() {
            return this.user;
        }

        @Override
        public String getPassword() {
            return this.password;
        }
    }

    /**
     * Returns the properties as they would be after successfully applying the changes to the existing (old) set of properties.
     */
    public static Map<String, String> getNewProperties (
            Map<String, String> oldProperties, Map<String, String> changedProperties) {
        Map<String, String> newCompleteProperties = new HashMap<>(oldProperties);
        for (Map.Entry<String, String> changedProperty : changedProperties.entrySet()) {
            if (changedProperty.getValue() == null) {
                // Null values represent properties which are deleted by the user (might also have never existed actually)
                newCompleteProperties.remove(changedProperty.getKey());
            } else {
                newCompleteProperties.put(changedProperty.getKey(), changedProperty.getValue());
            }
        }
        return newCompleteProperties;
    }

    public static boolean isRefreshNeeded(Map<String, String> newProperties) {
        return newProperties.containsKey(PROP_CONNECTION_ADDRESS)
                || newProperties.containsKey(PROP_CONNECTION_NAME)
                || newProperties.containsKey(PROP_USERNAME)
                || newProperties.containsKey(PROP_SCHEMA_NAME)
                || newProperties.containsKey(PROP_TABLES)
                || newProperties.containsKey(PROP_HDFS_URL);
    }


    public static List<String> getTableFilter(Map<String, String> properties) {
        String tableNames = getProperty(properties, PROP_TABLES, "");
        if (!tableNames.isEmpty()) {
            List<String> tables = Arrays.asList(tableNames.split(","));
            for (int i=0; i<tables.size();++i) {
                tables.set(i, tables.get(i).trim());
            }
            return tables;
        } else {
            return null;
        }
    }

}


