package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.HdfsSerDeExportService;
import com.exasol.hadoop.hcat.HCatMetadataService;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hdfs.HdfsService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import com.exasol.utils.UdfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
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
        String hcatAddress = iter.getString(PARAM_IDX_HCAT_ADDRESS);
        String hdfsUser = iter.getString(PARAM_IDX_HDFS_USER);
        String hdfsServerUrls = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_HDFS_ADDRESS, "");
        String staticPartition = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_STATIC_PARTITION, "");
        String dynamicPartitionExaCols = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_DYNAMIC_PARTITION_EXA_COLS, "");
        String authType = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_TYPE, "");
        String connName = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_CONNECTION, "");
        String compressionType = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_COMPRESSION_TYPE, "");
        String debugAddress = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_DEBUG_ADDRESS, "");
        int firstColumnIndex = PARAM_IDX_FIRST_DATA_COLUMN;

        // Optional: Define a udf debug service address to which stdout will be redirected.
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

        // Check partition specification
        boolean hasStaticPartition = false;
        boolean hasDynamicPartition = false;
        List<HCatTableColumn> hivePartitions = tableMeta.getPartitionColumns();
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

        // If the user defined an webHDFS URL (e.g. "webhdfs://domain.namenode:50070" or "hdfs://namenode:8020") we use this and ignore the hdfs URL returned from HCat (e.g. "hdfs://namenode:8020")
        // Two use cases: 1) Use webHDFS instead of HDFS and 2) If Hadoop returns a namenode hostname unreachable from EXASOL (e.g. not fully-qualified) we can overwrite e.g. by "hdfs://domain.namenode:8020"
        List<String> hdfsAddresses = hdfsServerUrls.equals("") ? Arrays.asList(tableMeta.getHdfsAddress()) : Arrays.asList(hdfsServerUrls.split(","));

        UserGroupInformation ugi = useKerberos ?
                KerberosHadoopUtils.getKerberosUGI(kerberosCredentials) : UserGroupInformation.createRemoteUser(hdfsUser);
        FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws Exception {
                final Configuration conf = new Configuration();
                if (useKerberos) {
                    conf.set("dfs.namenode.kerberos.principal", hdfsUser);
                }
                return HdfsService.getFileSystem(hdfsAddresses, conf);
            }
        });

        // HDFS path
        StringBuilder hdfsPath = new StringBuilder();
        hdfsPath.append(fs.getUri());
        hdfsPath.append(tableMeta.getHdfsTableRootPath());

        // Create partitions if necessary
        List<Integer> dynamicPartitionExaColNums = new ArrayList<>();
        if (hasStaticPartition) {
            // Static partition
            HCatMetadataService.createTablePartitionIfNotExists(hcatDB, hcatTable, staticPartition, hcatAddress, hdfsUser, useKerberos, kerberosCredentials);
            hdfsPath.append("/").append(staticPartition);
        } else if (hasDynamicPartition) {
            // Dynamic partition
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

        // Parquet filename
        // Format: 'exa_export_<yyyyMMdd_HHmmss>_<UUID>.parq'
        //             <yyyyMMdd_HHmmss>: current date
        //             <UUID>: random UUID
        StringBuilder file = new StringBuilder();
        file.append("exa_export_");
        file.append(new SimpleDateFormat("yyyyMMdd_HHmmss_").format(new Date()));
        file.append(UUID.randomUUID().toString().replaceAll("-", ""));
        file.append(".parq");

        String fileFormat = tableMeta.getSerDeClass();
        if (fileFormat.toLowerCase().contains("parquet")) {
            HdfsSerDeExportService.exportToParquetTable(hdfsPath.toString(), hdfsUser, useKerberos, kerberosCredentials, file.toString(), tableMeta, compressionType, null, firstColumnIndex, dynamicPartitionExaColNums, iter, null, null);
        } else {
            throw new RuntimeException("The file format is unsupported: " + fileFormat);
        }
    }

}
