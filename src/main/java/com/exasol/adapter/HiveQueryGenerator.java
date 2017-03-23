package com.exasol.adapter;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.sql.SqlStatementSelect;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.SQLException;

public class HiveQueryGenerator {

    private static final String KRB_KEY = "ExaAuthType=Kerberos;";

    public static String getOutputSql(SchemaMetadataInfo meta, Table table, String partitionString, String outputColumnsString,
                                      String selectedColumnsString,String selectListPart ,String secondPart, ExaConnectionInformation connection) throws SQLException {
        String credentialsAndConn = "";
        credentialsAndConn += "HCAT_DB = '" + HiveAdapterProperties.getSchema(meta.getProperties()) + "'";
        credentialsAndConn += " HCAT_TABLE = '" + table.getTableName().toLowerCase()+ "'";
        credentialsAndConn += " HCAT_ADDRESS = '" + connection.getAddress() + "'";
        credentialsAndConn += " HDFS_USER = '" + connection.getUser() + "'";
        if(!partitionString.equals("")){
            credentialsAndConn += " PARTITIONS = '" + partitionString + "'";
        }
        if(!outputColumnsString.equals("")) {
            credentialsAndConn += " OUTPUT_COLUMNS = '" + outputColumnsString.toLowerCase() + "'";
        }
        if(isKerberosAuth(connection.getPassword())){
            credentialsAndConn += " AUTH_TYPE = 'kerberos'";
            credentialsAndConn += " AUTH_KERBEROS_CONNECTION = '" + HiveAdapterProperties.getConnectionName(meta.getProperties()) + "'";
        }

        return  "SELECT " + selectListPart  + " FROM (IMPORT INTO (" + selectedColumnsString + ")" +
                " FROM SCRIPT ADAPTERSCHEMA.IMPORT_HCAT_TABLE WITH " + credentialsAndConn +")" + secondPart;
    }

    public static String getWhereClause(SqlStatementSelect select,SqlGeneratorForWhereClause sqlGeneratorForWhereClause) throws AdapterException {
        String sql = "";
        if (select.hasFilter()) {
            sql +=" WHERE ";
            sql+=select.getWhereClause().accept(sqlGeneratorForWhereClause);
        }
        return sql;
    }

    public static boolean isKerberosAuth(String pass) {
        if (pass==null) {
            return false;
        }
        return pass.indexOf(KRB_KEY) == 0;
    }
}
