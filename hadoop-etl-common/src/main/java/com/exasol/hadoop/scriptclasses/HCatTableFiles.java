package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.hcat.HCatMetadataService;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hcat.WebHCatJsonSerializer;
import com.exasol.hadoop.hdfs.HdfsService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.utils.UdfUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Main UDF entry point. Per convention, the UDF Script must have the same name as the main class.
 */
public class HCatTableFiles {
    
    private static final int PARAM_IDX_HCAT_DB = 0;
    private static final int PARAM_IDX_HCAT_TABLE = 1;
    private static final int PARAM_IDX_HCAT_ADDRESS = 2;
    private static final int PARAM_IDX_HDFS_USER = 3;
    private static final int PARAM_IDX_PARALLELISM = 4;
    private static final int PARAM_IDX_PARTITIONS = 5;
    private static final int PARAM_IDX_OUTPUT_COLUMNS = 6;
    private static final int PARAM_IDX_HDFS_ADDRESS = 7;
    private static final int PARAM_IDX_AUTH_TYPE = 8;
    private static final int PARAM_IDX_AUTH_KERBEROS_CONNECTION = 9;
    private static final int PARAM_IDX_DEBUG_ADDRESS = 10;
    
    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {

        System.setProperty("sun.security.krb5.debug", "true");

        String hcatDB = iter.getString(PARAM_IDX_HCAT_DB);
        String hcatTable = iter.getString(PARAM_IDX_HCAT_TABLE);
        String hCatAddress = iter.getString(PARAM_IDX_HCAT_ADDRESS);
        String hdfsAndHCatUser = iter.getString(PARAM_IDX_HDFS_USER);
        int parallelism = iter.getInteger(PARAM_IDX_PARALLELISM);
        // Optional parameters
        String partitionFilterSpec = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_PARTITIONS, "");
        String outputColumnsSpec = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_OUTPUT_COLUMNS, "");
        String hdfsAddressesFromUser = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_HDFS_ADDRESS, "");
        List<String> hdfsAddresses = new ArrayList<>();
        if(!hdfsAddressesFromUser.equals("")) {
            String[] additionalHdfsAddresses = hdfsAddressesFromUser.split(",");
            for(int i=0; i<additionalHdfsAddresses.length; i++){
                hdfsAddresses.add(additionalHdfsAddresses[i]);
            }
        }
        String authType = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_TYPE, "");
        boolean useKerberos = authType.equalsIgnoreCase("kerberos");
        String connName = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_KERBEROS_CONNECTION, "");
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
        // Optional: Define a udf debug service address to which stdout will be redirected
        String debugAddress = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_DEBUG_ADDRESS, "");
        if (! debugAddress.isEmpty()) {
            try {
                String debugHost = debugAddress.split(":")[0];
                int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
                UdfUtils.attachToOutputService(debugHost, debugPort);
            } catch (Exception ex) {
                throw new RuntimeException("You have to specify a valid hostname and port for the udf debug service, e.g. 'hostname:3000'");
            }
        }

        HCatTableMetadata tableMeta = HCatMetadataService.getMetadataForTable(
            hcatDB,
            hcatTable,
            hCatAddress,
            hdfsAndHCatUser,
            useKerberos,
            kerberosCredentials);
        
        // Check table type (must be managed or external, no view or index tables)
        String tableType = tableMeta.getTableType();
        if (!tableType.equalsIgnoreCase("MANAGED_TABLE") && !tableType.equalsIgnoreCase("EXTERNAL_TABLE") ) {
            throw new RuntimeException("Table type " + tableType + " is not supported. Only managed and external tables are supported.");
        }

        // If the user defined an webHDFS URL (e.g. "webhdfs://domain.namenode:50070" or "hdfs://namenode:8020") we use this and ignore the hdfs URL returned from HCat (e.g. "hdfs://namenode:8020")
        // Two use cases: 1) Use webHDFS instead of HDFS and 2) If Hadoop returns a namenode hostname unreachable from EXASOL (e.g. not fully-qualified) we can overwrite e.g. by "hdfs://domain.namenode:8020"
        if(hdfsAddresses.isEmpty()){
            hdfsAddresses.add(tableMeta.getHdfsAddress());
        }

        List<String> filePaths = HdfsService.getFilesFromTable(
                hdfsAndHCatUser,
                tableMeta.getHdfsTableRootPath(),
                partitionFilterSpec,
                tableMeta.getPartitionColumns(),
                useKerberos,
                kerberosCredentials,
                hdfsAddresses);

        int numFilePaths = filePaths.size();
        for (int i = 0; i < numFilePaths; i++) {
            iter.emit(
                    StringUtils.join(hdfsAddresses, ","),
                    filePaths.get(i),
                    hdfsAndHCatUser,
                    tableMeta.getInputFormatClass(),
                    tableMeta.getSerDeClass(),
                    WebHCatJsonSerializer.serializeColumnArray(tableMeta.getColumns()),
                    WebHCatJsonSerializer.serializeColumnArray(tableMeta.getPartitionColumns()),
                    WebHCatJsonSerializer.serializeSerDeParameters(tableMeta.getSerDeParameters()),
                    i % parallelism,
                    authType,
                    connName,
                    outputColumnsSpec,
                    debugAddress);
        }
    }
}
