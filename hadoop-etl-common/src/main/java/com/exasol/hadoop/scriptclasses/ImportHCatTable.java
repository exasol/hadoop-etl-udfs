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

        // Optional Parameters
        String hdfsUser = getParameter(params, "HDFS_USER", "");
        String hcatUser = getParameter(params, "HCAT_USER", "");
        String parallelism = getParameter(params, "PARALLELISM", "nproc()");
        String partitions = getParameter(params, "PARTITIONS", "");
        String outputColumnsSpec = getParameter(params, "OUTPUT_COLUMNS", "");
        String hdfsURLs = getParameter(params, "HDFS_URL", "");
        String authenticationType = getParameter(params, "AUTH_TYPE", "");
        String kerberosConnection = getParameter(params, "KERBEROS_CONNECTION", "");
        String kerberosHCatServicePrincipal = getParameter(params, "KERBEROS_HCAT_SERVICE_PRINCIPAL", "");
        String kerberosHdfsServicePrincipal = getParameter(params, "KERBEROS_HDFS_SERVICE_PRINCIPAL", "");
        String enableRCPEncryption = getParameter(params, "ENABLE_RPC_ENCRYPTION", "false");
        String debugAddress = getParameter(params, "DEBUG_ADDRESS", "");

        boolean useKerberos = authenticationType.equalsIgnoreCase("kerberos");
        checkAuthParameters(hdfsUser, hcatUser, kerberosHCatServicePrincipal, kerberosHdfsServicePrincipal, useKerberos);
        String hcatUserOrServicePrincipal = useKerberos ? kerberosHCatServicePrincipal : hcatUser;
        String hdfsUserOrServicePrincipal = useKerberos ? kerberosHdfsServicePrincipal : hdfsUser;

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
        hcatUDFArgs.add("'" + hdfsUserOrServicePrincipal + "'");
        hcatUDFArgs.add("'" + hcatUserOrServicePrincipal + "'");
        hcatUDFArgs.add(parallelism);
        hcatUDFArgs.add("'" + partitions + "'");
        hcatUDFArgs.add("'" + outputColumnsSpec + "'");
        hcatUDFArgs.add("'" + hdfsURLs + "'");
        hcatUDFArgs.add("'" + authenticationType + "'");
        hcatUDFArgs.add("'" + kerberosConnection + "'");
        hcatUDFArgs.add("'" + enableRCPEncryption + "'");
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
        importUDFArgs.add("enable_rpc_encryption");
        importUDFArgs.add("debug_address");

        String sql = "SELECT"
                + " " + meta.getScriptSchema() +".IMPORT_HIVE_TABLE_FILES(" + Joiner.on(", ").join(importUDFArgs) + ")"
                + emitsSpec
                + " FROM ("
                + " SELECT " + meta.getScriptSchema() +".HCAT_TABLE_FILES(" + Joiner.on(", ").join(hcatUDFArgs) + ")"
                + ") GROUP BY import_partition;";

        return sql;
    }

    static void checkAuthParameters(String hdfsUser, String hcatUser, String kerberosHCatServicePrincipal, String kerberosHdfsServicePrincipal, boolean useKerberos) {
        if (useKerberos) {
            if (!hdfsUser.isEmpty()
                    || !hcatUser.isEmpty()
                    || kerberosHCatServicePrincipal.isEmpty()
                    || kerberosHdfsServicePrincipal.isEmpty()) {
                throw new RuntimeException("If authentication type kerberos is specified, the properties KERBEROS_HCAT_SERVICE_PRINCIPAL and KERBEROS_HDFS_SERVICE_PRINCIPAL have to be specified and HDFS_USER and HCAT_USER must be empty.");
            }
        } else {
            if (hdfsUser.isEmpty()
                    || hcatUser.isEmpty()
                    || !kerberosHCatServicePrincipal.isEmpty()
                    || !kerberosHdfsServicePrincipal.isEmpty()) {
                throw new RuntimeException("If standard basic authentication type is specified (no kerberos), the properties HDFS_USER and HCAT_USER have to be specified and KERBEROS_HCAT_SERVICE_PRINCIPAL and KERBEROS_HDFS_SERVICE_PRINCIPAL must be empty.");
            }
        }
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
