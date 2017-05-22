package com.exasol.hadoop;

import com.exasol.ExaIteratorDummy;
import com.exasol.ExaMetadataDummy;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.io.File;

public class HdfsSerDeExportServiceTest {

    /**
     * Execute
     * ssh ws64-2.dev.exasol.com -L 9083:vm031.cos.dev.exasol.com:9083 -L 8020:vm031.cos.dev.exasol.com:8020 -L 8888:vm031.cos.dev.exasol.com:8888
     * before running this test
     */
    @Test
    public void exportToTable() throws Exception {
        String dbname = "default";
        //String table = "sample_07_parquet";
        String table = "parquet_all_types";
        String hiveMetastoreURL = "thrift://cloudera01.exacloud.de:9083";
        String hdfsURL = "hdfs://cloudera01.exacloud.de:8020/user/hive/warehouse/" + table;
        HCatTableMetadata tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, dbname, table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        List<List<Object>> rows = new ArrayList<>();
        List<Class<?>> rowTypes = new ArrayList<>();
        List<Object> rowValues = new ArrayList<>();
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
        rows.add(rowValues);

        String hdfsUser = "hdfs";
        String filename = "test.parq";
        File testFile = new File(filename);
        testFile.delete();
        HdfsSerDeExportService.exportToParquetTableTest(hdfsURL, hdfsUser, filename, tableMeta, new ExaIteratorDummy(rows), new ExaMetadataDummy(rowTypes));
    }

}