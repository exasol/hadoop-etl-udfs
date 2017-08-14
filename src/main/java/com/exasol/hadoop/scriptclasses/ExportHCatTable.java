package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaExportSpecification;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.hcat.HCatMetadataService;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import com.exasol.utils.JdbcUtils;
import com.exasol.utils.UdfUtils;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.util.*;

/**
 * Main UDF entry point. Per convention, the UDF Script must have the same name
 * as the main class.
 */
public class ExportHCatTable {

    public static String generateSqlForExportSpec(ExaMetadata meta, ExaExportSpecification exportSpec) {

        // Mandatory parameters
        Map<String, String> params = exportSpec.getParameters();
        String hcatDB = getMandatoryParameter(params, "HCAT_DB");
        String hcatTable = getMandatoryParameter(params, "HCAT_TABLE");
        String hcatAddress = getMandatoryParameter(params, "HCAT_ADDRESS");
        String hdfsUser = getMandatoryParameter(params, "HDFS_USER");

        // Optional parameters
        String hdfsAddress = getParameter(params, "HDFS_URL", "");
        String staticPartition = getParameter(params, "STATIC_PARTITION", "");
        String dynamicPartitionExaCols = getParameter(params, "DYNAMIC_PARTITION_EXA_COLS", "");
        String authenticationType = getParameter(params, "AUTH_TYPE", "");
        String kerberosConnection = getParameter(params, "AUTH_KERBEROS_CONNECTION", "");
        String jdbcAuthType = getParameter(params, "JDBC_AUTH_TYPE", "");
        String jdbcConnection = getParameter(params, "JDBC_CONNECTION", "");
        String compressionType = getParameter(params, "COMPRESSION_TYPE", "uncompressed");
        String debugAddress = getParameter(params, "DEBUG_ADDRESS", "");

        // Only used for unit testing
        String unitTestMode = getParameter(params, "UNIT_TEST_MODE", "");
        boolean isUnitTestMode = unitTestMode.equals("true");

        if (!debugAddress.isEmpty()) {
            try {
                String debugHost = debugAddress.split(":")[0];
                int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                UdfUtils.attachToOutputService(debugHost, debugPort);
            } catch (Exception ex) {
                throw new RuntimeException("You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }

        if (staticPartition != null && !staticPartition.isEmpty() &&
            dynamicPartitionExaCols != null && !dynamicPartitionExaCols.isEmpty()) {
            throw new RuntimeException("Static and dynamic partitions cannot both be specified.");
        }

        // JDBC statements
        List<String> jdbcSqlStatements = getJdbcStatements(exportSpec, hcatDB, hcatTable);
        if (jdbcSqlStatements.size() > 0) {
            ExaConnectionInformation jdbcConn = getJdbcConnection(meta, jdbcConnection);
            boolean useKerberos = jdbcAuthType.equalsIgnoreCase("kerberos");
            JdbcUtils.executeJdbcStatements(jdbcSqlStatements, jdbcConn, useKerberos);
        }

        // Dynamic partitions
        List<String> exaColNames = getExaSourceColumnNames(exportSpec);
        List<Integer> dynamicPartsExaColNums;
        if (dynamicPartitionExaCols != null && !dynamicPartitionExaCols.isEmpty()) {
            // Dynamic columns were specified
            dynamicPartsExaColNums = getExaColumnNumbersOfSpecifiedDynamicPartitions(dynamicPartitionExaCols, exaColNames);
        } else {
            // Dynamic columns were not specified
            boolean useKerberos = authenticationType.equalsIgnoreCase("kerberos");
            KerberosCredentials kerberosCredentials = null;
            if (!kerberosConnection.isEmpty()) {
                ExaConnectionInformation kerberosConn;
                try {
                    kerberosConn = meta.getConnection(kerberosConnection);
                } catch (ExaConnectionAccessException e) {
                    throw new RuntimeException("Exception while accessing Kerberos connection " + kerberosConnection + ": " + e.toString(), e);
                }
                kerberosCredentials = new KerberosCredentials(kerberosConn);
            }
            HCatTableMetadata tableMeta;
            try {
                if (isUnitTestMode) {
                    // Don't try to connect to HCat service in unit test mode
                    tableMeta = new HCatTableMetadata("", new ArrayList<>(), new ArrayList<>(), "", "", "", "", new ArrayList<>());
                } else {
                    tableMeta = HCatMetadataService.getMetadataForTable(hcatDB, hcatTable, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception while fetching metadata for " + hcatTable + ": " + e.toString(), e);
            }
            dynamicPartsExaColNums = getExaColumnNumbersOfNonSpecifiedDynamicPartitions(tableMeta, exaColNames.size());
        }

        List<String> groupByColumns = new ArrayList<>();
        for (Integer dynamicPartsExaColNum : dynamicPartsExaColNums) {
            groupByColumns.add(exaColNames.get(dynamicPartsExaColNum));
        }

        // SQL query
        List<String> exportUDFArgs = new ArrayList<>();
        exportUDFArgs.add(hcatDB);
        exportUDFArgs.add(hcatTable);
        exportUDFArgs.add(hcatAddress);
        exportUDFArgs.add(hdfsUser);
        exportUDFArgs.add(hdfsAddress);
        exportUDFArgs.add(staticPartition);
        exportUDFArgs.add(Joiner.on(",").join(dynamicPartsExaColNums));
        exportUDFArgs.add(authenticationType);
        exportUDFArgs.add(kerberosConnection);
        exportUDFArgs.add(compressionType);
        exportUDFArgs.add(debugAddress);

        // SQL
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append("\"").append(meta.getScriptSchema()).append("\".\"EXPORT_INTO_HIVE_TABLE\"");
        sql.append("(");
        sql.append("'").append(Joiner.on("', '").join(exportUDFArgs)).append("'");
        sql.append(", ");
        sql.append("\"").append(Joiner.on("\", \"").join(exaColNames)).append("\"");
        sql.append(") ");
        sql.append("FROM ");
        sql.append("DUAL"); // Dummy placeholder
        if (!groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ");
            sql.append("\"").append(Joiner.on("\", \"").join(groupByColumns)).append("\"");
        }
        sql.append(";");

        return sql.toString();
    }

    private static List<String> getExaSourceColumnNames(ExaExportSpecification exportSpec) {
        List<String> colNames = new ArrayList<>();
        List<String> sourceColNames = exportSpec.getSourceColumnNames();

        for (String sourceColName : sourceColNames) {
            String colName;
            String[] colNameSplit = sourceColName.split("\\.");
            // Check for and remove table name
            if (colNameSplit.length > 1) {
                colName = colNameSplit[1];
            } else {
                colName = colNameSplit[0];
            }
            colName = colName.replaceAll("\"", "");
            colNames.add(colName);
        }

        return colNames;
    }

    private static List<Integer> getExaColumnNumbersOfSpecifiedDynamicPartitions(String dynamicPartitionExaCols,
                                                                                 List<String> exaColNames) {
        List<String> dynamicCols = new ArrayList<>();
        String[] dynamicPartitions = dynamicPartitionExaCols.split("/");
        for (int i = 0; i < dynamicPartitions.length; i++) {
            String dynPartCol;
            String[] dynPartTableCol = dynamicPartitions[i].split("\\.");

            // Check for and remove table name
            if (dynPartTableCol.length > 1) {
                dynPartCol = dynPartTableCol[1];
            } else {
                dynPartCol = dynPartTableCol[0];
            }

            if (dynPartCol.startsWith("\"") && dynPartCol.endsWith("\"")) {
                // Quoted identifier, case sensitive
                dynPartCol = dynPartCol.replaceAll("\"", "");
            } else {
                // Not quoted, to upper case
                dynPartCol = dynPartCol.toUpperCase();
            }

            dynamicCols.add(dynPartCol);
        }

        List<Integer> dynamicPartsExaColNums = new ArrayList<>();
        for (String dynamicCol : dynamicCols) {
            int exaColIndex = exaColNames.indexOf(dynamicCol);
            if (exaColIndex == -1) {
                throw new RuntimeException("Dynamic partition " + dynamicCol + " was not found in column list");
            }
            dynamicPartsExaColNums.add(exaColIndex);
        }

        return dynamicPartsExaColNums;
    }

    private static List<Integer> getExaColumnNumbersOfNonSpecifiedDynamicPartitions(HCatTableMetadata tableMeta, int numExaCols) {
        List<Integer> dynamicPartsExaColNums = new ArrayList<>();

        // Use last columns for partitions
        int numHivePartitions = tableMeta.getPartitionColumns().size();
        if (numHivePartitions > 0) {
            if (numHivePartitions > numExaCols) {
                throw new RuntimeException("The target table has " + numHivePartitions + " although the source table only has " + numExaCols + " partitions.");
            }
            Integer[] exaColumnNums = new Integer[numHivePartitions];
            int firstPartColNum = numExaCols - numHivePartitions;
            for (int i = 0; i < exaColumnNums.length; i++) {
                exaColumnNums[i] = firstPartColNum + i;
            }
            dynamicPartsExaColNums = Arrays.asList(exaColumnNums);
        }

        return dynamicPartsExaColNums;
    }

    private static ExaConnectionInformation getJdbcConnection(ExaMetadata meta, String jdbcConnection) {
        if (jdbcConnection.isEmpty()) {
            throw new RuntimeException("The JDBC_CONNECTION parameter is required, but was not specified.");
        }
        ExaConnectionInformation jdbcConn;
        try {
            jdbcConn = meta.getConnection(jdbcConnection);
        } catch (ExaConnectionAccessException e) {
            throw new RuntimeException("ExaConnectionAccessException while getting connection " + jdbcConnection + ": " + e.toString(), e);
        }
        return jdbcConn;
    }

    private static List<String> getJdbcStatements(ExaExportSpecification exportSpec, String hcatDB, String hcatTable) {
        List<String> jdbcSqlStatements = new ArrayList<>();
        if (exportSpec.hasTruncate()) {
            StringBuilder sb = new StringBuilder();
            sb.append("TRUNCATE TABLE ");
            if (hcatDB != null && !hcatDB.isEmpty()) {
                sb.append(hcatDB + ".");
            }
            sb.append(hcatTable);
            jdbcSqlStatements.add(sb.toString());
        }
        if (exportSpec.hasReplace()) {
            StringBuilder sb = new StringBuilder();
            sb.append("DROP TABLE ");
            if (hcatDB != null && !hcatDB.isEmpty()) {
                sb.append(hcatDB + ".");
            }
            sb.append(hcatTable);
            jdbcSqlStatements.add(sb.toString());
        }
        if (exportSpec.hasCreatedBy()) {
            jdbcSqlStatements.add(exportSpec.getCreatedBy());
        }
        return jdbcSqlStatements;
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
