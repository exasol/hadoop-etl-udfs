package com.exasol.adapter;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveTableInformation {


    public static Table getHiveTable(HiveMetaStoreClient hiveMetastoreClient, String tableName, String databaseName){
        Table table;
        try {
            table = hiveMetastoreClient.getTable(databaseName, tableName);
        } catch (MetaException e) {
            throw new RuntimeException("Unknown MetaException occured when reading table information for table " + tableName + " in database " +databaseName +  ": " + e.toString(), e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException("Table " + tableName + " in database " + databaseName +  ". Error: " + e.toString(), e);
        } catch (org.apache.thrift.TException e) {
            throw new RuntimeException("Unknown TException occured when reading table information for table " + tableName + " in database " + databaseName +  ": " + e.toString(), e);
        }
        return table;
    }

    public static DataType typeMapping(String  hiveType) throws SQLException {

        DataType colType;
        if(hiveType.toLowerCase().startsWith("array")||hiveType.toLowerCase().startsWith("map")||hiveType.toLowerCase().startsWith("struct")|| hiveType.toLowerCase().equals("string")
                || hiveType.equals("binary")){
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
        }
        else if(hiveType.toLowerCase().equals("tinyint")){
            colType = DataType.createDecimal(3,0);
        }
        else if(hiveType.toLowerCase().equals("smallint")){
            colType = DataType.createDecimal(5,0);
        }
        else if(hiveType.toLowerCase().equals("int")){
            colType = DataType.createDecimal(10,0);
        }
        else if(hiveType.toLowerCase().equals("bigint")){
            colType = DataType.createDecimal(20,0);
        }
        else if(hiveType.toLowerCase().equals("boolean")){
            colType = DataType.createBool();
        }
        else if(hiveType.toLowerCase().startsWith("char")){
            int size = new Integer(hiveType.substring(hiveType.indexOf("(")+1,hiveType.indexOf(")")));
            DataType.ExaCharset charset = DataType.ExaCharset.UTF8;
            colType = DataType.createChar(size,charset);
        }
        else if(hiveType.toLowerCase().startsWith("decimal")){
            String sizeElements = hiveType.substring(hiveType.indexOf("(")+1,hiveType.indexOf(")"));
            int decimalPrecision = new Integer(sizeElements.substring(0,sizeElements.indexOf(",")));
            int decimalScale = new Integer(sizeElements.substring(sizeElements.indexOf(",")+1));
            colType = DataType.createDecimal(decimalPrecision,decimalScale);
        }
        else if(hiveType.toLowerCase().equals("double")||hiveType.equals("float")){
            colType = DataType.createDouble();
        }
        else if(hiveType.toLowerCase().equals("timestamp")){
            colType = DataType.createTimestamp(false);
        }
        else if(hiveType.toLowerCase().equals("date")){
            colType = DataType.createDate();
        }
        else if(hiveType.toLowerCase().startsWith("varchar")){
            int size = new Integer(hiveType.substring(hiveType.indexOf("(")+1,hiveType.indexOf(")")));
            colType = DataType.createVarChar(size, DataType.ExaCharset.UTF8);
        }
        else{
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
        }

        return colType;
    }


    public static TableMetadata getTableMetadataFromHCatTableMetadata(String tableName, HCatTableMetadata hcatTableMetadata) throws MetadataException {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        List<HCatTableColumn> partitionColumns = hcatTableMetadata.getPartitionColumns();
        addColumsToList(columns,hcatTableMetadata.getColumns(),false);
        addColumsToList(columns,partitionColumns,true);
        return new TableMetadata(tableName, "", columns, "");
    }

    public static void addColumsToList(List<ColumnMetadata> columns, List<HCatTableColumn> inputColums,boolean isPartition){
        for (HCatTableColumn column : inputColums) {

            try {
                DataType dataType = typeMapping(column.getDataType());
                // TODO nice to have: derive nullable/isIdentity/defaultValue/comment from HCatalog (not important)
                ColumnAdapterNotes columnAdapterNotes = new ColumnAdapterNotes(column.getDataType(),isPartition);
                columns.add(new ColumnMetadata(column.getName().toUpperCase(),ColumnAdapterNotes.serialize(columnAdapterNotes), dataType, false, false, "", ""));
            }catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static HCatTableMetadata getHCatTableMetadata(Table table){
        StorageDescriptor sd = table.getSd();
        String location = sd.getLocation();
        String inputFormat = sd.getInputFormat();
        String outputFormat = sd.getOutputFormat();
        String serDeClass = sd.getSerdeInfo().getSerializationLib();
        List<FieldSchema> cols = sd.getCols();

        List<HCatTableColumn> columns = new ArrayList<>();
        for (FieldSchema col : cols) {

            columns.add(new HCatTableColumn(col.getName(), col.getType()));
        }

        String tableType = table.getTableType();
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        List<HCatTableColumn> partitionColumns = new ArrayList<>();
        for (FieldSchema partitionKey : partitionKeys) {
            partitionColumns.add(new HCatTableColumn(partitionKey.getName(), partitionKey.getType()));
        }

        Map<String, String> parameters = sd.getSerdeInfo().getParameters();
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        for (String key : parameters.keySet()) {
            serDeParameters.add(new HCatSerDeParameter(key, parameters.get(key)));
        }

        return new HCatTableMetadata(location, columns, partitionColumns, tableType, inputFormat, outputFormat, serDeClass, serDeParameters);
    }
}
