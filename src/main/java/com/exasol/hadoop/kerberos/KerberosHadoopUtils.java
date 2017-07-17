package com.exasol.hadoop.kerberos;

import com.exasol.utils.UdfUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.nio.charset.Charset;

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

    public static void configKerberosJaas(String path, String user, String password) throws Exception {
        final String krbKey = "ExaAuthType=Kerberos";
        String[] confKeytab = password.split(";");
        if (confKeytab.length != 3 || !confKeytab[0].equals(krbKey)) {
            throw new RuntimeException("An invalid Kerberos CONNECTION was specified.");
        }

        String confPath = UdfUtils.writeTempFile(DatatypeConverter.parseBase64Binary(confKeytab[1]), path, "krb5_", ".conf");
        String keytabPath = UdfUtils.writeTempFile(DatatypeConverter.parseBase64Binary(confKeytab[2]), path, "kt_", ".keytab");

        StringBuilder jaasData = new StringBuilder();
        jaasData.append("Client {\n");
        jaasData.append("com.sun.security.auth.module.Krb5LoginModule required\n");
        jaasData.append("principal=\"" + user + "\"\n");
        jaasData.append("useKeyTab=true\n");
        jaasData.append("keyTab=\"" + keytabPath + "\"\n");
        jaasData.append("doNotPrompt=true\n");
        jaasData.append("useTicketCache=false;\n");
        jaasData.append("};\n");
        jaasData.append("com.sun.security.jgss.initiate {\n");
        jaasData.append("com.sun.security.auth.module.Krb5LoginModule required\n");
        jaasData.append("principal=\"" + user + "\"\n");
        jaasData.append("useKeyTab=true\n");
        jaasData.append("keyTab=\"" + keytabPath + "\"\n");
        jaasData.append("doNotPrompt=true\n");
        jaasData.append("useTicketCache=false;\n");
        jaasData.append("};\n");
        String jaasPath = UdfUtils.writeTempFile(jaasData.toString().getBytes(Charset.forName("UTF-8")), path, "jaas_", ".conf");

        System.setProperty("java.security.auth.login.config", jaasPath);
        System.setProperty("java.security.krb5.conf", confPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        // Set login user. The value is actually not important, but something must be specified.
        // UnixLoginModule makes a native system call to get the username
        int endIndex = StringUtils.indexOfAny(user, "/@");
        if (endIndex != -1) {
            user = user.substring(0, endIndex);
        }
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(user));
    }
}
