package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaImportSpecification;
import com.exasol.ExaMetadata;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Main UDF entry point. Per convention, the UDF Script must have the same name
 * as the main class.
 */
public class ImportHCatTable {

    public static String generateSqlForImportSpec(ExaMetadata meta, ExaImportSpecification importSpec) {

        // Mandatory Parameters
        Map<String, String> params = importSpec.getParameters();
        String hcatDB = getMandatoryParameter(params, "HCAT_DB");
        String hcatTable = getMandatoryParameter(params, "HCAT_TABLE");
        String hCatAddress = getMandatoryParameter(params, "HCAT_ADDRESS");
        String hdfsUser = getMandatoryParameter(params, "HDFS_USER");
        String hcatUser = getMandatoryParameter(params, "HCAT_USER");  // If non-kerberos: use for createRemoteUser(), if kerberos: use as hdfs service principle
        
        // Optional Parameters
        String parallelism = getParameter(params, "PARALLELISM", "nproc()");
        String partitions = getParameter(params, "PARTITIONS", "");
        String outputColumnsSpec = getParameter(params, "OUTPUT_COLUMNS", "");
        String hdfsURLs = getParameter(params, "HDFS_URL", "");
        String authenticationType = getParameter(params, "AUTH_TYPE", "");
        String kerberosConnection = getParameter(params, "AUTH_KERBEROS_CONNECTION", "");
        String debugAddress = getParameter(params, "DEBUG_ADDRESS", "");
        
        // Construct EMITS specification
        String emitsSpec = "";  // "EMITS (col1 INT, col2 varchar(100))"
        if (importSpec.isSubselect()) {
            if (importSpec.getSubselectColumnNames().size()>0) {
                // We always need the emits specification in case of subselect and Hadoop ETL UDFs
                StringBuilder emitsBuilder = new StringBuilder();
                emitsBuilder.append(" EMITS (");
                boolean first = true;
                for (int i=0; i<importSpec.getSubselectColumnNames().size(); i++) {
                    String colName = importSpec.getSubselectColumnNames().get(i);
                    if (!first) {
                        emitsBuilder.append(",");
                    }
                    emitsBuilder.append("\"" + colName + "\" " + importSpec.getSubselectColumnSqlTypes().get(i));
                    first = false;
                }
                emitsBuilder.append(")");
                emitsSpec = emitsBuilder.toString();
            } else {
                throw new RuntimeException("In case of IMPORT in a subselect you need to specify the output columns, e.g. 'INSERT INTO (a int, b varchar(100)) ...'.");
            }
        }
        
        // Connection is not supported
        if (importSpec.hasConnectionName() || importSpec.hasConnectionInformation()) {
            throw new RuntimeException("Specifying connections is not supported for this Script");
        }
        
        // Argument list
        List<String> hcatUDFArgs = new ArrayList<>();
        hcatUDFArgs.add("'" + hcatDB + "'");
        hcatUDFArgs.add("'" + hcatTable + "'");
        hcatUDFArgs.add("'" + hCatAddress + "'");
        hcatUDFArgs.add("'" + hdfsUser + "'");
        hcatUDFArgs.add("'" + hcatUser + "'");
        hcatUDFArgs.add(parallelism);
        hcatUDFArgs.add("'" + partitions + "'");
        hcatUDFArgs.add("'" + outputColumnsSpec + "'");
        hcatUDFArgs.add("'" + hdfsURLs + "'");
        hcatUDFArgs.add("'" + authenticationType + "'");
        hcatUDFArgs.add("'" + kerberosConnection + "'");
        hcatUDFArgs.add("'" + debugAddress + "'");
        
        List<String> importUDFArgs = new ArrayList<>();
        importUDFArgs.add("hdfspath");
        importUDFArgs.add("input_format");
        importUDFArgs.add("serde");
        importUDFArgs.add("column_info");
        importUDFArgs.add("partition_info");
        importUDFArgs.add("serde_props");
        importUDFArgs.add("hdfs_server_port");
        importUDFArgs.add("hdfs_user");
        importUDFArgs.add("auth_type");
        importUDFArgs.add("conn_name");
        importUDFArgs.add("output_columns");
        importUDFArgs.add("debug_address");

        String sql = "SELECT"
                + " " + meta.getScriptSchema() +".IMPORT_HIVE_TABLE_FILES(" + Joiner.on(", ").join(importUDFArgs) + ")"
                + emitsSpec
                + " FROM ("
                + " SELECT " + meta.getScriptSchema() +".HCAT_TABLE_FILES(" + Joiner.on(", ").join(hcatUDFArgs) + ")"
                + ") GROUP BY import_partition;";

        return sql;
    }
    
    private static String getMandatoryParameter(Map<String, String> params, String key) {
        if (!params.containsKey(key)) {
            throw new RuntimeException("The mandatory property " + key + " was not defined. Please specify it and run the statement again.");
        }
        return params.get(key);
    }
    
    private static String getParameter(Map<String, String> params, String key, String defaultValue) {
        if (!params.containsKey(key)) {
            return defaultValue;
        } else {
            return params.get(key);
        }
    }
}
