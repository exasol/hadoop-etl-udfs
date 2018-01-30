package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.HdfsSerDeImportService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.utils.UdfUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main UDF entry point. Per convention, the UDF Script must have the same name as the main class.
 */
public class ImportHiveTableFiles {
    
    private static final int PARAM_IDX_FILE = 0;
    private static final int PARAM_IDX_INPUT_FORMAT = 1;
    private static final int PARAM_IDX_SERDE = 2;
    private static final int PARAM_IDX_COLUMNS = 3;
    private static final int PARAM_IDX_PARTITIONS = 4;
    private static final int PARAM_IDX_SERDE_PROPS = 5;
    private static final int PARAM_IDX_HDFS_ADDRESS = 6;
    private static final int PARAM_IDX_HDFS_USER = 7;
    private static final int PARAM_IDX_AUTH_TYPE = 8;
    private static final int PARAM_IDX_AUTH_KERBEROS_CONNECTION = 9;
    private static final int PARAM_IDX_OUTPUT_COLUMNS = 10;
    private static final int PARAM_IDX_DEBUG_ADDRESS = 11;

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {

        System.setProperty("sun.security.krb5.debug", "true");

        String inputFormatClassName = iter.getString(PARAM_IDX_INPUT_FORMAT);
        String serDeClassName = iter.getString(PARAM_IDX_SERDE);
        String colInfo = iter.getString(PARAM_IDX_COLUMNS);
        String partInfo = iter.getString(PARAM_IDX_PARTITIONS);
        String serdeProps = iter.getString(PARAM_IDX_SERDE_PROPS);
        String hdfsServerUrls = iter.getString(PARAM_IDX_HDFS_ADDRESS);
        String hdfsUser = iter.getString(PARAM_IDX_HDFS_USER);
        List<String> hdfsAddresses = Arrays.asList(hdfsServerUrls.split(","));

        // Optional: Kerberos authentication
        boolean useKerberos = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_TYPE, "").equalsIgnoreCase("kerberos");
        KerberosCredentials kerberosCredentials = null;
        String connName = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_AUTH_KERBEROS_CONNECTION, "");
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
        
        // Optional: Specify which columns to load, including JsonPath.
	    String outputColumnsSpec = UdfUtils.getOptionalStringParameter(meta, iter, PARAM_IDX_OUTPUT_COLUMNS, "");
        
        // Optional: Define a udf debug service address to which stdout will be redirected.
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

        List<String> files = new ArrayList<>();
        do {
            files.add(iter.getString(PARAM_IDX_FILE));
        } while (iter.next());
        HdfsSerDeImportService.importFiles(files, inputFormatClassName, serDeClassName, serdeProps, colInfo, partInfo, outputColumnsSpec, hdfsAddresses, hdfsUser, useKerberos, kerberosCredentials, iter);
    }
}
