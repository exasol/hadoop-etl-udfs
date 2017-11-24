package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.HdfsSerDeExportService;
import com.exasol.hadoop.hcat.HCatMetadataService;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.utils.UdfUtils;

import java.text.SimpleDateFormat;
import java.util.*;

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
    private static final int PARAM_IDX_COMPRESSION_TYPE = 9;
    private static final int PARAM_IDX_DEBUG_ADDRESS = 10;
    private static final int PARAM_IDX_FIRST_DATA_COLUMN = 11;

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        String hcatDB = iter.getString(PARAM_IDX_HCAT_DB);
        String hcatTable = iter.getString(PARAM_IDX_HCAT_TABLE);
        String hdfsUrl = iter.getString(PARAM_IDX_HDFS_ADDRESS);
        String hdfsUser = iter.getString(PARAM_IDX_HDFS_USER);
        String hcatAddress = iter.getString(PARAM_IDX_HCAT_ADDRESS);
        String staticPartition = iter.getString(PARAM_IDX_STATIC_PARTITION);
        String dynamicPartitionExaCols = iter.getString(PARAM_IDX_DYNAMIC_PARTITION_EXA_COLS);
        String authType = iter.getString(PARAM_IDX_AUTH_TYPE);
        String connName = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_CONNECTION, "");
        String compressionType = iter.getString(PARAM_IDX_COMPRESSION_TYPE);
        String debugAddress = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_DEBUG_ADDRESS, "");
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
            kerberosCredentials = new KerberosCredentials(kerberosConnection);
        }

        HCatTableMetadata tableMeta = HCatMetadataService.getMetadataForTable(hcatDB, hcatTable, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
        System.out.println("tableMeta: " + tableMeta);

        boolean hasStaticPartition = false;
        boolean hasDynamicPartition = false;
        List<HCatTableColumn> hivePartitions = tableMeta.getPartitionColumns();
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
            if (hivePartitions.isEmpty()) {
                throw new RuntimeException("A static partition was specified although the target table does not have any partitions.");
            }
            hasStaticPartition = true;
        } else if (!dynamicPartitionExaCols.isEmpty()) {
            // Dynamic partition
            hasDynamicPartition = true;
        } else if (!dynamicPartitionExaCols.isEmpty() && hivePartitions.isEmpty()) {
            throw new RuntimeException("A dynamic partition was specified although the target table does not have any partitions.");
        }

        // HDFS path
        StringBuilder hdfsPath = new StringBuilder();
        hdfsPath.append(tableMeta.getHdfsAddress());
        hdfsPath.append(tableMeta.getHdfsTableRootPath());

        // Partitions
        List<Integer> dynamicPartitionExaColNums = new ArrayList<>();
        if (hasStaticPartition) {
            HCatMetadataService.createTablePartitionIfNotExists(hcatDB, hcatTable, staticPartition, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
            hdfsPath.append("/").append(staticPartition);
        } else if (hasDynamicPartition) {
            int[] exaColumnNums = new int[hivePartitions.size()];
            String[] exaColumnNumsStr = dynamicPartitionExaCols.split(",");
            for (int i = 0; i < exaColumnNumsStr.length; i++) {
                exaColumnNums[i] = Integer.parseInt(exaColumnNumsStr[i]);
            }

            if (hivePartitions.size() != exaColumnNums.length) {
                String exMsg = "The target table has " + hivePartitions.size() + " partition columns, but "
                        + exaColumnNums.length + " source partition columns were specified.";
                throw new RuntimeException(exMsg);
            }

            // Build partitions
            StringBuilder dynamicPart = new StringBuilder();
            for (int i = 0; i < exaColumnNums.length; i++) {
                int colNum = firstColumnIndex + exaColumnNums[i];
                dynamicPartitionExaColNums.add(colNum);
                dynamicPart.append("/").append(hivePartitions.get(i).getName()).append("=").append(iter.getString(colNum));
            }
            String dynamicPartition = dynamicPart.toString();
            HCatMetadataService.createTablePartitionIfNotExists(hcatDB, hcatTable, dynamicPartition, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
            hdfsPath.append(dynamicPartition);
        }

        // Filename
        StringBuilder file = new StringBuilder();
        file.append("exa_export_");
        file.append(new SimpleDateFormat("yyyyMMdd_HHmmss_").format(new Date()));
        file.append(UUID.randomUUID().toString().replaceAll("-", ""));
        file.append(".parq");

        String fileFormat = tableMeta.getSerDeClass();
        if (fileFormat.toLowerCase().contains("parquet")) {
            HdfsSerDeExportService.exportToParquetTable(hdfsPath.toString(), hdfsUser, useKerberos, kerberosCredentials, file.toString(), tableMeta, compressionType, null, firstColumnIndex, dynamicPartitionExaColNums, iter);
        } else {
            throw new RuntimeException("The file format is unsupported: " + fileFormat);
        }
    }

}
