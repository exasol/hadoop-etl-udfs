package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaExportSpecification;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        String partitions = getParameter(params, "PARTITIONS", "");
        String authenticationType = getParameter(params, "AUTH_TYPE", "");
        String kerberosConnection = getParameter(params, "AUTH_KERBEROS_CONNECTION", "");
        String jdbcConnection = getParameter(params, "JDBC_CONNECTION", "");
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

        // JDBC statements
        List<String> jdbcSqlStatements = new ArrayList<>();
        if (exportSpec.hasTruncate()) {
            jdbcSqlStatements.add("TRUNCATE TABLE " + hcatDB + "." + hcatTable);
        }
        if (exportSpec.hasReplace()) {
            jdbcSqlStatements.add("DROP TABLE " + hcatDB + "." + hcatTable);
        }
        if (exportSpec.hasCreateBy()) {
            jdbcSqlStatements.add(exportSpec.getCreateBy());
        }
        if (jdbcSqlStatements.size() > 0) {
            if (jdbcConnection.isEmpty()) {
                throw new RuntimeException("The JDBC_CONNECTION parameter is required, but was not specified.");
            }
            ExaConnectionInformation jdbcConn;
            try {
                jdbcConn = meta.getConnection(jdbcConnection);
            } catch (ExaConnectionAccessException e) {
                throw new RuntimeException("ExaConnectionAccessException while getting connection " + jdbcConnection + ": " + e.toString(), e);
            }

            boolean useKerberosJdbc = (jdbcConn.getPassword().indexOf("ExaAuthType=Kerberos") == 0);
            String user = jdbcConn.getUser();
            String password = jdbcConn.getPassword();
            if (useKerberosJdbc) {
                try {
                    configKerberos(jdbcConn.getUser(), jdbcConn.getPassword());
                    user = "";
                    password = "";
                } catch (Exception e) {
                    throw new RuntimeException("Exception while configuring Kerberos connection " + jdbcConnection + ": " + e.toString(), e);
                }
            }
            executeJdbcStatements(jdbcConn.getAddress(), user, password, jdbcSqlStatements);
        }

        // SQL query
        List<String> exportUDFArgs = new ArrayList<>();
        exportUDFArgs.add(hcatDB);
        exportUDFArgs.add(hcatTable);
        exportUDFArgs.add(hcatAddress);
        exportUDFArgs.add(hdfsUser);
        exportUDFArgs.add(hdfsAddress);
        exportUDFArgs.add(partitions);
        exportUDFArgs.add(authenticationType);
        exportUDFArgs.add(kerberosConnection);
        exportUDFArgs.add(debugAddress);

        String sql;
        sql = "SELECT " + meta.getScriptSchema() + ".EXPORT_INTO_HIVE_TABLE(";
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
        sql += ";";

        return sql;
    }

    private static void executeJdbcStatements(String url, String user, String password, List<String> sqlStatements) {
        final String jdbcClass = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(jdbcClass);
        } catch (ExceptionInInitializerError e) {
            throw new RuntimeException("ExceptionInInitializerError while creating table using JDBC driver: " + e.toString(), e);
        } catch (LinkageError e) {
            throw new RuntimeException("LinkageError while creating table using JDBC driver: " + e.toString(), e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClassNotFoundException while creating table using JDBC driver: " + e.toString(), e);
        }

        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            for (String sql : sqlStatements) {
                stmt.executeUpdate(sql);
            }
        } catch (SQLTimeoutException e) {
            throw new RuntimeException("SQLTimeoutException while creating table using JDBC driver: " + e.toString(), e);
        } catch (SQLException e) {
            throw new RuntimeException("SQLException while creating table using JDBC driver: " + e.toString(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println("SQLException in Statement.close() while creating table using JDBC driver: " + e.toString());
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println("SQLException in Connection.close() while creating table using JDBC driver: " + e.toString());
            }
        }
    }

    private static void configKerberos(String user, String password) throws Exception {
        final String krbKey = "ExaAuthType=Kerberos;";
        try {
            password = password.replaceFirst(krbKey, "");
        } catch (Exception e) {
            throw new RuntimeException("Could not find " + krbKey + " in password: " + e.getMessage());
        }
        String[] confKeytab = password.split(";");
        if (confKeytab.length != 2) {
            throw new RuntimeException("Invalid Kerberos conf/keytab");
        }

        File kerberosBaseDir = new File("/tmp");
        File krbDir = File.createTempFile("kerberos_", null, kerberosBaseDir);
        krbDir.delete();
        krbDir.mkdir();
        krbDir.deleteOnExit();

        String krbConfPath = writeKrbConf(krbDir, confKeytab[0]);
        String keytabPath = writeKeytab(krbDir, confKeytab[1]);
        String jaasConfigPath = writeJaasConfig(krbDir, user, keytabPath);
        System.setProperty("java.security.auth.login.config", jaasConfigPath);
        System.setProperty("java.security.krb5.conf", krbConfPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        // Set login user. The value is actually not important, but something must be specified.
        // UnixLoginModule makes a native system to get the username
        int endIndex = StringUtils.indexOfAny(user, "/@");
        if (endIndex != -1) {
            user = user.substring(0, endIndex);
        }
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(user));
    }

    private static String writeKrbConf(File krbDir, String base64Conf) throws Exception {
        File file = File.createTempFile("krb_", ".conf", krbDir);
        file.deleteOnExit();
        FileOutputStream os = new FileOutputStream(file);
        os.write(DatatypeConverter.parseBase64Binary(base64Conf));
        os.close();
        return file.getCanonicalPath();
    }

    private static String writeKeytab(File krbDir, String base64Keytab) throws Exception {
        File file = File.createTempFile("kt_", ".keytab", krbDir);
        file.deleteOnExit();
        FileOutputStream os = new FileOutputStream(file);
        os.write(DatatypeConverter.parseBase64Binary(base64Keytab));
        os.close();
        return file.getCanonicalPath();
    }

    private static String writeJaasConfig(File krbDir, String princ, String keytabPath) throws Exception {
        File file = File.createTempFile("jaas_", ".conf", krbDir);
        file.deleteOnExit();
        String jaasData;
        jaasData  = "Client {\n";
        jaasData += "com.sun.security.auth.module.Krb5LoginModule required\n";
        jaasData += "principal=\"" + princ + "\"\n";
        jaasData += "useKeyTab=true\n";
        jaasData += "keyTab=\"" + keytabPath + "\"\n";
        jaasData += "doNotPrompt=true\n";
        jaasData += "useTicketCache=false;\n";
        jaasData += "};\n";
        jaasData += "com.sun.security.jgss.initiate {\n";
        jaasData += "com.sun.security.auth.module.Krb5LoginModule required\n";
        jaasData += "principal=\"" + princ + "\"\n";
        jaasData += "useKeyTab=true\n";
        jaasData += "keyTab=\"" + keytabPath + "\"\n";
        jaasData += "doNotPrompt=true\n";
        jaasData += "useTicketCache=false;\n";
        jaasData += "};\n";
        FileOutputStream os = new FileOutputStream(file);
        os.write(jaasData.getBytes(Charset.forName("UTF-8")));
        os.close();
        return file.getCanonicalPath();
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
