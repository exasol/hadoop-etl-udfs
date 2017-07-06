package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.HdfsSerDeExportService;
import com.exasol.hadoop.hcat.HCatMetadataService;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.utils.UdfUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Main UDF entry point. Per convention, the UDF Script must have the same name as the main class.
 */
public class ExportIntoHiveTable {

    private static final int PARAM_IDX_HCAT_DB = 0;
    private static final int PARAM_IDX_HCAT_TABLE = 1;
    private static final int PARAM_IDX_HCAT_ADDRESS = 2;
    private static final int PARAM_IDX_HDFS_USER = 3;
    private static final int PARAM_IDX_HDFS_ADDRESS = 4;
    private static final int PARAM_IDX_STATIC_PARTITION = 5;
    private static final int PARAM_IDX_DYNAMIC_PARTITION_EXA_COLS = 6;
    private static final int PARAM_IDX_AUTH_TYPE = 7;
    private static final int PARAM_IDX_AUTH_CONNECTION = 8;
    private static final int PARAM_IDX_FILE_FORMAT = 9;
    private static final int PARAM_IDX_COMPRESSION_TYPE = 10;
    private static final int PARAM_IDX_DEBUG_ADDRESS = 11;
    private static final int PARAM_IDX_FIRST_DATA_COLUMN = 12;

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        String hcatDB = iter.getString(PARAM_IDX_HCAT_DB);
        String hcatTable = iter.getString(PARAM_IDX_HCAT_TABLE);
        String hdfsUrl = iter.getString(PARAM_IDX_HDFS_ADDRESS);
        String hdfsUser = iter.getString(PARAM_IDX_HDFS_USER);
        String hcatAddress = iter.getString(PARAM_IDX_HCAT_ADDRESS);
        String staticPartition = iter.getString(PARAM_IDX_STATIC_PARTITION);
        String dynamicPartitionExaCols = iter.getString(PARAM_IDX_DYNAMIC_PARTITION_EXA_COLS);
        String authType = iter.getString(PARAM_IDX_AUTH_TYPE);
        String connName = iter.getString(PARAM_IDX_AUTH_CONNECTION);
        String fileFormat = iter.getString(PARAM_IDX_FILE_FORMAT);
        String compressionType = iter.getString(PARAM_IDX_COMPRESSION_TYPE);
        String debugAddress = iter.getString(PARAM_IDX_DEBUG_ADDRESS);
        int firstColumnIndex = PARAM_IDX_FIRST_DATA_COLUMN;

        if (!debugAddress.isEmpty()) {
            try {
                String debugHost = debugAddress.split(":")[0];
                int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                UdfUtils.attachToOutputService(debugHost, debugPort);
            } catch (Exception ex) {
                throw new RuntimeException("You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }

        boolean useKerberos = authType.equalsIgnoreCase("kerberos");
        KerberosCredentials kerberosCredentials = null;
        if (!connName.isEmpty()) {
            ExaConnectionInformation kerberosConnection = meta.getConnection(connName);
            String principal = kerberosConnection.getUser();
            final String krbKey = "ExaAuthType=Kerberos";
            String[] confKeytab = kerberosConnection.getPassword().split(";");
            if (confKeytab.length != 3 || !confKeytab[0].equals(krbKey)) {
                throw new RuntimeException("An invalid Kerberos CONNECTION was specified.");
            }
            byte[] configFile = UdfUtils.base64ToByteArray(confKeytab[1]);
            byte[] keytabFile = UdfUtils.base64ToByteArray(confKeytab[2]);
            kerberosCredentials = new KerberosCredentials(principal, configFile, keytabFile);
        }

        HCatTableMetadata tableMeta = HCatMetadataService.getMetadataForTable(hcatDB, hcatTable, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
        System.out.println("tableMeta: " + tableMeta);

        boolean hasStaticPartition = false;
        boolean hasDynamicPartition = false;
        boolean tableHasPartition = !tableMeta.getPartitionColumns().isEmpty();
        if (staticPartition == null) {
            staticPartition = "";
        }
        if (dynamicPartitionExaCols == null) {
            dynamicPartitionExaCols = "";
        }
        if (!staticPartition.isEmpty() && !dynamicPartitionExaCols.isEmpty()) {
            throw new RuntimeException("A static and dynamic partition cannot both be specified.");
        } else if (!staticPartition.isEmpty()) {
            // Static partition
            if (!tableHasPartition) {
                throw new RuntimeException("A partition was specified although the target table does not have any partitions.");
            }
            hasStaticPartition = true;
        } else {
            // Possible dynamic partition
            if (tableHasPartition) {
                // Dynamic partition
                hasDynamicPartition = true;
            } else {
                if (!dynamicPartitionExaCols.isEmpty()) {
                    throw new RuntimeException("A partition was specified although the target table does not have any partitions.");
                }
                // No dynamic partition
            }
        }

        // HDFS path
        StringBuilder sb = new StringBuilder();
        sb.append(tableMeta.getHdfsAddress());
        sb.append(tableMeta.getHdfsTableRootPath());
        if (hasStaticPartition) {
            HCatMetadataService.createTablePartitionIfNotExists(hcatDB, hcatTable, staticPartition, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
            //HiveMetastoreService.createPartitionIfNotExists(hcatAddress, useKerberos, kerberosCredentials.getPrinciple(), hcatDB, hcatTable, staticPartition);
            //HiveMetastoreService.createPartitionIfNotExists(hcatAddress, useKerberos, hdfsUser, hcatDB, hcatTable, staticPartition);
            sb.append("/" + staticPartition);
        }
        String hdfsPath = sb.toString();

        // Filename
        sb = new StringBuilder();
        sb.append("exa_export_");
        sb.append(new SimpleDateFormat("yyyyMMdd_HHmmss_").format(new Date()));
        sb.append(UUID.randomUUID().toString().replaceAll("-", ""));
        sb.append(".parq");
        String file = sb.toString();

        if (fileFormat.equals("PARQUET")) {
            HdfsSerDeExportService.exportToParquetTable(hdfsPath, hdfsUser, useKerberos, kerberosCredentials, file, tableMeta, compressionType, null, firstColumnIndex, iter);
        } else {
            throw new RuntimeException("The file format is unsupported: " + fileFormat);
        }
    }

}
