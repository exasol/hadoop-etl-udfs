package com.exasol.utils;

import com.exasol.ExaConnectionInformation;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;

import java.sql.*;
import java.util.List;

/**
 * Uses the Hive JDBC driver to execute necessary statements for Export into Hive.
 */
public class JdbcUtils {
    public static String HIVE_JDBC_CLASS = "org.apache.hive.jdbc.HiveDriver";

    public static void executeJdbcStatements(List<String> sqlStatements, ExaConnectionInformation jdbcConn, boolean useKerberos) {
        String user = jdbcConn.getUser();
        String password = jdbcConn.getPassword();
        String url = jdbcConn.getAddress();
        if (useKerberos) {
            try {
                final String path = "/tmp";
                KerberosHadoopUtils.configKerberosJaas(path, user, password);
                user = "";
                password = "";
            } catch (Exception e) {
                throw new RuntimeException("Exception while configuring Kerberos connection " + url + ": " + e.toString(), e);
            }
        }

        try {
            Class.forName(HIVE_JDBC_CLASS);
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

}
