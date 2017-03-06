package com.exasol.adapter;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.sql.SqlStatementSelect;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.SQLException;
import java.util.List;

public class HiveAdapterUtils {

    private static final String KRB_KEY = "ExaAuthType=Kerberos;";

    public static String getOutputSql(SchemaMetadataInfo meta, Table table, String partitionString, String outputColumnsString,
                                       String selectListPart ,String secondPart, ExaConnectionInformation connection) throws SQLException {
        String credentialsAndConn = "";
        if(outputColumnsString.equals("")){
            StorageDescriptor sd = table.getSd();
            List<FieldSchema> cols = sd.getCols();
            for(FieldSchema col:cols){
                outputColumnsString += col.getName() + " " + HiveTableInformation.typeMapping(col.getType()) + ",";
            }
            List<FieldSchema> partitions = table.getPartitionKeys();
            for(FieldSchema col:partitions){
                outputColumnsString += col.getName() + " " + HiveTableInformation.typeMapping(col.getType()) + ",";
            }

            outputColumnsString = outputColumnsString.substring(0,outputColumnsString.length()-1);
        }
            credentialsAndConn += "HCAT_DB = '" + HiveAdapterProperties.getSchema(meta.getProperties()) + "'";
            credentialsAndConn += " HCAT_TABLE = '" + table.getTableName()+ "'";
            credentialsAndConn += " HCAT_ADDRESS = '" + connection.getAddress() + "'";
            credentialsAndConn += " HDFS_USER = '" + connection.getUser() + "'";
            if(!partitionString.equals("")){
                credentialsAndConn += " PARTITIONS = '" + partitionString + "'";
            }
                credentialsAndConn += " OUTPUT_COLUMNS = '(" + outputColumnsString + ")'";
        if(isKerberosAuth(connection.getPassword())){
            credentialsAndConn += " AUTH_TYPE = 'kerberos'";
            credentialsAndConn += " AUTH_KERBEROS_CONNECTION = '" + HiveAdapterProperties.getConnectionName(meta.getProperties()) + "'";
        }

        return  "SELECT " + selectListPart  + " FROM (IMPORT INTO " + outputColumnsString +
                " FROM SCRIPT ADAPTERSCHEMA.IMPORT_HCAT_TABLE WITH " + credentialsAndConn +")" + secondPart;
    }

    public static String getWhereClause(SqlStatementSelect select,SqlGeneratorForWhereClause sqlGeneratorForWhereClause){
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
