package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.ExaIteratorDummy;
import com.exasol.ExaMetadata;
import com.exasol.ExaMetadataDummy;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.exasol.hadoop.scriptclasses.ExportIntoHiveTable;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;
import parquet.schema.DecimalMetadata;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.File;

public class HdfsSerDeExportServiceTest {

    /**
     * Execute
     * ssh ws64-2.dev.exasol.com -L 9083:vm031.cos.dev.exasol.com:9083 -L 8020:vm031.cos.dev.exasol.com:8020 -L 8888:vm031.cos.dev.exasol.com:8888
     * before running this test
     */
    @Test
    public void exportToTableUdf() throws Exception {
        String dbName = "default";
        String table = "parquet_partition";
        String hiveMetastoreURL = "thrift://cloudera01.exacloud.de:9083";
        String hdfsUser = "hdfs";
        String hdfsUrl = "";
        String partition = "part1=a/part2=1";
        String authType = "";
        String authKerberosConnection = "";
        String debugAddress = "";

        List<List<Object>> iterValues = new ArrayList<>();
        List<Object> iterValue = new ArrayList<>();
        iterValue.add(dbName);
        iterValue.add(table);
        iterValue.add(hiveMetastoreURL);
        iterValue.add(hdfsUser);
        iterValue.add(hdfsUrl);
        iterValue.add(partition);
        iterValue.add(authType);
        iterValue.add(authKerberosConnection);
        iterValue.add(debugAddress);
        iterValues.add(iterValue);

        ExaMetadata meta = null;
        ExaIterator iter = new ExaIteratorDummy(iterValues);
        ExportIntoHiveTable.run(meta, iter);
    }

    //@Test
    public void exportToTable() throws Exception {
        String dbName = "default";
        //String table = "sample_07_parquet";
        String table = "parquet_partition";
        String hiveMetastoreURL = "thrift://cloudera01.exacloud.de:9083";
        String hdfsURL = "hdfs://cloudera01.exacloud.de:8020/user/hive/warehouse/" + table;
        String partition = "part1=a/part2=1";
        if (partition != null && !partition.isEmpty())
            hdfsURL += "/" + partition;
        HCatTableMetadata tableMeta = null;
        tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, dbName, table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        List<List<Object>> rows = new ArrayList<>();
        List<Class<?>> rowTypes = new ArrayList<>();
        List<Object> rowValues = new ArrayList<>();

        /*
        // PARQUET_NUM_TYPES
        rowTypes.add(Class.forName("java.lang.Integer"));       rowValues.add(55);
        rowTypes.add(Class.forName("java.lang.Integer"));       rowValues.add(5555);
        rowTypes.add(Class.forName("java.lang.Integer"));       rowValues.add(555555555);
        rowTypes.add(Class.forName("java.lang.Long"));          rowValues.add(555555555555555555L);
        //rowTypes.add(Class.forName("java.lang.Float"));       //rowValues.add(55.55f);
        rowTypes.add(Class.forName("java.lang.Double"));        rowValues.add(55.55);
        rowTypes.add(Class.forName("java.lang.Double"));        rowValues.add(55555.55555);
        rowTypes.add(Class.forName("java.math.BigDecimal"));    rowValues.add(new BigDecimal("55555555555555555555555555555555555555"));
        rowTypes.add(Class.forName("java.math.BigDecimal"));    rowValues.add(new BigDecimal("555555555555555555555555555555555.55555"));
        rowTypes.add(Class.forName("java.math.BigDecimal"));    rowValues.add(new BigDecimal("0.12345678"));

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "ti", OriginalType.INT_8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "si", OriginalType.INT_16));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "i", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "bi", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "f", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "d", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16,"dec1"));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16,"dec2"));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 4,"dec3"));
        */

        /*
        // PARQUET STRING TYPES
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("str_str_str_str");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("c");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("c_c_c_c");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("v");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("v_v_v_v");

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "str", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c2", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "vc2", OriginalType.UTF8));
        */

        /*
        // PARQUET DATE TIME TYPES
        rowTypes.add(Class.forName("java.sql.Timestamp"));  rowValues.add(Timestamp.valueOf("1945-05-24 02:01:01.123456789"));

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "ts", null));
        */

        /*
        // PARQUET BOOLEAN BINARY TYPES
        rowTypes.add(Class.forName("java.lang.Boolean"));   rowValues.add(false);
        rowTypes.add(Class.forName("java.lang.String"));    rowValues.add("fdsa");

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "bool", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "bin", null));
        */

        ///*
        // PARQUET SNAPPY TYPES
        rowTypes.add(Class.forName("java.lang.Integer"));   rowValues.add(1111);
        rowTypes.add(Class.forName("java.lang.String"));    rowValues.add("aaaa");

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "c1", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c2", null));
        //*/

        rows.add(rowValues);

        //String testPartition = "part1=d/part2=4";
        //boolean createdPartition = HiveMetastoreService.createPartitionIfNotExists(hiveMetastoreURL,false, "", dbName, table, testPartition);
        //System.out.println("Created partition " + testPartition + ": " + createdPartition);

        String hdfsUser = "hdfs";
        StringBuilder sb = new StringBuilder();
        sb.append("exa_export_");
        sb.append(new SimpleDateFormat("yyyyMMdd_HHmmss_").format(new Date()));
        sb.append(UUID.randomUUID().toString().replaceAll("-", ""));
        sb.append(".parq");
        String filename = sb.toString();
        System.out.println("Temp Filename: " + filename);

        //HdfsSerDeExportService.exportToParquetTableTest(hdfsURL, hdfsUser, filename, tableMeta, schemaTypes, new ExaIteratorDummy(rows));
    }

}
