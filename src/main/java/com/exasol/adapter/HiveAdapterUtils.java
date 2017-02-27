package com.exasol.adapter;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by np on 2/15/2017.
 */
public class HiveAdapterUtils {

    private static final String KRB_KEY = "ExaAuthType=Kerberos;";

    public static Table getHiveTable(HiveMetaStoreClient hiveMetastoreClient, String tableName, String databaseName){
        Table table;
        try {
            table = hiveMetastoreClient.getTable(databaseName, tableName.toLowerCase());
        } catch (MetaException e) {
            throw new RuntimeException("Unknown MetaException occured when reading table information for table " + tableName + " in database " +databaseName +  ": " + e.toString(), e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException("Table " + tableName + " in database " + databaseName +  ". Error: " + e.toString(), e);
        } catch (org.apache.thrift.TException e) {
            throw new RuntimeException("Unknown TException occured when reading table information for table " + tableName + " in database " + databaseName +  ": " + e.toString(), e);
        }
        return table;
    }

    public static String getOutputSql(SchemaMetadataInfo meta, Table table, String partitionString, String outputColumnsString,
                                       String selectPart,String secondPart, ExaConnectionInformation connection) throws SQLException {
        String credentialsAndConn = "";
        if(outputColumnsString.equals("")){
            StorageDescriptor sd = table.getSd();
            List<FieldSchema> cols = sd.getCols();
            for(FieldSchema col:cols){
                outputColumnsString += col.getName() + " " + typeMapping(col.getType()) + ",";
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

        return  "SELECT " + selectPart + " FROM (IMPORT INTO " + outputColumnsString +
                " FROM SCRIPT ADAPTERSCHEMA.IMPORT_HCAT_TABLE WITH " + credentialsAndConn +")" + secondPart;
    }

     public static DataType typeMapping(String  hiveType) throws SQLException {

        DataType colType;
        if(hiveType.startsWith("array")||hiveType.startsWith("map")||hiveType.startsWith("struct")|| hiveType.equals("string")
                || hiveType.equals("binary")){
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
        }
        else if(hiveType.equals("tinyint")){
            colType = DataType.createDecimal(3,0);
        }
        else if(hiveType.equals("smallint")){
            colType = DataType.createDecimal(5,0);
        }
        else if(hiveType.equals("int")){
            colType = DataType.createDecimal(10,0);
        }
        else if(hiveType.equals("bigint")){
            colType = DataType.createDecimal(20,0);
        }
        else if(hiveType.equals("boolean")){
            colType = DataType.createBool();
        }
        else if(hiveType.startsWith("char")){
            int size = new Integer(hiveType.substring(hiveType.indexOf("(")+1,hiveType.indexOf(")")));
            DataType.ExaCharset charset = DataType.ExaCharset.UTF8;
            colType = DataType.createChar(size,charset);
        }
        else if(hiveType.startsWith("decimal")){
            String sizeElements = hiveType.substring(hiveType.indexOf("(")+1,hiveType.indexOf(")"));
            int decimalPrecision = new Integer(sizeElements.substring(0,sizeElements.indexOf(",")));
            int decimalScale = new Integer(sizeElements.substring(sizeElements.indexOf(",")+1));
            colType = DataType.createDecimal(decimalPrecision,decimalScale);
        }
        else if(hiveType.equals("double")||hiveType.equals("float")){
            colType = DataType.createDouble();
        }
        else if(hiveType.equals("timestamp")){
            colType = DataType.createTimestamp(false);
        }
        else if(hiveType.equals("date")){
            colType = DataType.createDate();
        }
        else if(hiveType.startsWith("varchar")){
            int size = new Integer(hiveType.substring(hiveType.indexOf("(")+1,hiveType.indexOf(")")));
            colType = DataType.createVarChar(size, DataType.ExaCharset.UTF8);
        }
        else{
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
        }

        return colType;
    }


    public static TableMetadata getTableMetadataFromHCatTableMetadata(String tableName, HCatTableMetadata hcatTableMetadata) {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        for (HCatTableColumn column : hcatTableMetadata.getColumns()) {

            try {
                DataType dataType = HiveAdapterUtils.typeMapping(column.getDataType());
                // TODO nice to have: derive nullable/isIdentity/defaultValue/comment from HCatalog (not important)

                columns.add(new ColumnMetadata(column.getName(), "", dataType, false, false, "", column.getDataType()));
            }catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return new TableMetadata(tableName, "", columns, "");
    }

     public static HCatTableMetadata getHCatTableMetadata(Table table){
         StorageDescriptor sd = table.getSd();
         String location = sd.getLocation();
         String inputFormat = sd.getInputFormat();
         String serDeClass = sd.getSerdeInfo().getSerializationLib();
         List<FieldSchema> cols = sd.getCols();

        List<HCatTableColumn> columns = new ArrayList<>();
        for (FieldSchema col : cols) {

            columns.add(new HCatTableColumn(col.getName().toUpperCase(), col.getType()));
        }

        String tableType = table.getTableType();
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        List<HCatTableColumn> partitionColumns = new ArrayList<>();
        for (FieldSchema partitionKey : partitionKeys) {
            partitionColumns.add(new HCatTableColumn(partitionKey.getName(), partitionKey.getType()));
            columns.add(new HCatTableColumn(partitionKey.getName().toUpperCase(), partitionKey.getType()));
        }

        Map<String, String> parameters = sd.getSerdeInfo().getParameters();
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        for (String key : parameters.keySet()) {
            serDeParameters.add(new HCatSerDeParameter(key, parameters.get(key)));
        }

        return new HCatTableMetadata(location, columns, partitionColumns, tableType, inputFormat, serDeClass, serDeParameters);
    }




     public static HiveMetaStoreClient getHiveMetastoreClient(ExaConnectionInformation connection){
        HiveMetaStoreClient hiveMetastoreClient;

        HiveConf hiveConf = new HiveConf(new Configuration(), HiveConf.class);
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, connection.getAddress());
        try {
            hiveMetastoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException("Unknown MetaException occured when connecting to the Hive Metastore " + e.getMessage(), e);
        }
        return hiveMetastoreClient;
    }

    public static String getSecondPartOfStatement(SqlStatementSelect select,SqlGenerator sqlGenerator,SqlGeneratorForWhereClause sqlGeneratorForWhereClause){
        String sql = "";
        if (select.hasFilter()) {
            sql +=" WHERE ";
            sql+=select.getWhereClause().accept(sqlGeneratorForWhereClause);
        }
        if (select.hasGroupBy()) {
            sql+=" GROUP BY ";
            sql+=select.getGroupBy().accept(sqlGenerator);
        }
        if (select.hasHaving()) {
            sql+=" HAVING ";
            sql+=select.getHaving().accept(sqlGenerator);
        }
        if (select.hasOrderBy()) {
            sql+=" ";
            sql+=select.getOrderBy().accept(sqlGenerator);
        }
        if (select.hasLimit()) {
            sql+=" ";
            sql+=select.getLimit().accept(sqlGenerator);
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
