package com.exasol.hadoop.kerberos;

import com.exasol.utils.UdfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;

public class KerberosHadoopUtils {

    public static UserGroupInformation getKerberosUGI(final KerberosCredentials kerberosCredentials) throws Exception {
        UserGroupInformation ugi;
        final String tmpDir = "/tmp";
        String confPath = writeTempConfFile(kerberosCredentials.getConfigFile(), tmpDir);
        System.setProperty("java.security.krb5.conf", confPath);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        String keytabPath = writeTempKeytabFile(kerberosCredentials.getKeytabFile(), tmpDir);
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosCredentials.getPrinciple(), keytabPath);
        new File(keytabPath).delete(); // Delete keytab file after login
        return ugi;
    }

    private static String writeTempConfFile(byte[] data, String path) throws Exception {
        return UdfUtils.writeTempFile(data, path, "krb5_", ".conf");
    }

    private static String writeTempKeytabFile(byte[] data, String path) throws Exception {
        return UdfUtils.writeTempFile(data, path, "kt_", ".keytab");
    }
}
