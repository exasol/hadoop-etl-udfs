package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaExportSpecification;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.hcat.HCatMetadataService;
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
        String hdfsAddress = getParameter(params, "HDFS_ADDRESS", "");
        String staticPartition = getParameter(params, "STATIC_PARTITION", "");
        String dynamicPartitionExaCols = getParameter(params, "DYNAMIC_PARTITION_EXA_COLS", "");
        String authenticationType = getParameter(params, "AUTH_TYPE", "");
        String kerberosConnection = getParameter(params, "AUTH_KERBEROS_CONNECTION", "");
        String jdbcAuthType = getParameter(params, "JDBC_AUTH_TYPE", "");
        String jdbcConnection = getParameter(params, "JDBC_CONNECTION", "");
        String fileFormat = getParameter(params, "FILE_FORMAT", "");
        String compressionType = getParameter(params, "COMPRESSION_TYPE", "uncompressed");
        String debugAddress = getParameter(params, "DEBUG_ADDRESS", "");

        if (!debugAddress.isEmpty()) {
            try {
                String debugHost = debugAddress.split(":")[0];
                int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                UdfUtils.attachToOutputService(debugHost, debugPort);
            } catch (Exception ex) {
                throw new RuntimeException("You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }

        fileFormat = fileFormat.toUpperCase();
        switch (fileFormat) {
            case "PARQUET":
                break;
            default:
                throw new RuntimeException("The " + fileFormat + " file format is unsupported.");
        }

        // JDBC statements
        List<String> jdbcSqlStatements = getJdbcStatements(exportSpec, hcatDB, hcatTable);
        if (jdbcSqlStatements.size() > 0) {
            ExaConnectionInformation jdbcConn = getJdbcConnection(meta, jdbcConnection);
            boolean useKerberos = jdbcAuthType.equalsIgnoreCase("kerberos");
            JdbcUtils.executeJdbcStatements(jdbcSqlStatements, jdbcConn, useKerberos);
        }

        // Dynamic partitions
        List<String> exaColumns = exportSpec.getSourceColumnNames();
        List<String> exaColNames = new ArrayList<>();
        for (String exaCol : exaColumns) {
            exaCol = exaCol.replaceAll("\"", "");
            exaColNames.add(exaCol);
        }

        List<Integer> dynamicPartsExaColNums = null;
        if (dynamicPartitionExaCols != null && !dynamicPartitionExaCols.isEmpty()) {
            List<String> dynamicCols = new ArrayList<>();
            String[] dynamicPartitions = dynamicPartitionExaCols.split("/");
            for (int i = 0; i < dynamicPartitions.length; i++) {
                String[] dynPartTableCol = dynamicPartitions[i].split("\\.");
                for (int j = 0; j < dynPartTableCol.length; j++) {
                    if (dynPartTableCol[j].startsWith("\"") && dynPartTableCol[j].endsWith("\"")) {
                        // Quoted identifier, case senstive
                        dynPartTableCol[j] = dynPartTableCol[j].replaceAll("\"", "");
                    } else {
                        // Not quoted, to upper case
                        dynPartTableCol[j] = dynPartTableCol[j].toUpperCase();
                    }
                }
                String table;
                String column;
                if (dynPartTableCol.length == 1) {
                    if (exportSpec.hasSourceTable()) {
                        table = exportSpec.getSourceTable().replaceAll("\"", "");
                    } else {
                        table = "";
                    }
                    column = dynPartTableCol[0];
                } else if (dynPartTableCol.length == 2) {
                    table = dynPartTableCol[0];
                    column = dynPartTableCol[1];
                } else {
                    throw new RuntimeException("Exception while parsing dynamic column name: " + dynamicPartitions[i]);
                }
                if (table.isEmpty()) {
                    dynamicCols.add(column);
                } else {
                    dynamicCols.add(table + "." + column);
                }
            }

            dynamicPartsExaColNums = new ArrayList<>();
            for (String dynamicCol : dynamicCols) {
                int exaColIndex = exaColNames.indexOf(dynamicCol);
                if (exaColIndex == -1) {
                    throw new RuntimeException("Dynamic partition " + dynamicCol + " was not found in column list");
                }
                dynamicPartsExaColNums.add(exaColIndex);
            }
        } else {
            // Possible non-specified dynamic partitions
            boolean useKerberos = authenticationType.equalsIgnoreCase("kerberos");
            KerberosCredentials kerberosCredentials = null;
            if (!kerberosConnection.isEmpty()) {
                ExaConnectionInformation kerberosConn;
                try {
                    kerberosConn = meta.getConnection(kerberosConnection);
                } catch (ExaConnectionAccessException e) {
                    throw new RuntimeException("Exception while accessing Kerberos connection " + kerberosConnection + ": " + e.toString(), e);
                }
                String principal = kerberosConn.getUser();
                final String krbKey = "ExaAuthType=Kerberos";
                String[] confKeytab = kerberosConn.getPassword().split(";");
                if (confKeytab.length != 3 || !confKeytab[0].equals(krbKey)) {
                    throw new RuntimeException("An invalid Kerberos CONNECTION was specified.");
                }
                byte[] configFile = UdfUtils.base64ToByteArray(confKeytab[1]);
                byte[] keytabFile = UdfUtils.base64ToByteArray(confKeytab[2]);
                kerberosCredentials = new KerberosCredentials(principal, configFile, keytabFile);
            }
            HCatTableMetadata tableMeta;
            try {
                tableMeta = HCatMetadataService.getMetadataForTable(hcatDB, hcatTable, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
            } catch (Exception e) {
                throw new RuntimeException("Exception while fetching metadata for " + hcatTable + ": " + e.toString(), e);
            }
            // Use last columns for partitions
            int numHivePartitions = tableMeta.getPartitionColumns().size();
            if (numHivePartitions > 0) {
                int numExaCols = exaColNames.size();
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
        }

        List<String> groupByColumns = new ArrayList<>();
        for (Integer dynamicPartsExaColNum : dynamicPartsExaColNums) {
            groupByColumns.add(exaColumns.get(dynamicPartsExaColNum));
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
        exportUDFArgs.add(fileFormat);
        exportUDFArgs.add(compressionType);
        exportUDFArgs.add(debugAddress);

        // SQL
        String sql;
        sql = "SELECT \"" + meta.getScriptSchema() + "\".\"EXPORT_INTO_HIVE_TABLE\"(";
        sql += "'" + Joiner.on("', '").join(exportUDFArgs) + "'";
        sql += ", ";
        sql += Joiner.on(", ").join(exportSpec.getSourceColumnNames());
        sql += ") ";
        sql += "FROM ";
        if (exportSpec.hasSourceTable()) {
            sql += exportSpec.getSourceTable();
        } else if (exportSpec.hasSourceSelectQuery()) {
            sql += "(" + exportSpec.getSourceSelectQuery() + ")";
        }
        if (!groupByColumns.isEmpty()) {
            sql += " GROUP BY ";
            sql += Joiner.on(", ").join(groupByColumns);
        }
        sql += ";";

        System.out.println("Export SQL: " + sql);
        return sql;
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
        if (exportSpec.hasCreateBy()) {
            jdbcSqlStatements.add(exportSpec.getCreateBy());
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
