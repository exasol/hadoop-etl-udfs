package com.exasol.hadoop;

import com.exasol.ExaIteratorDummy;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HdfsSerDeExportServiceTest {

    /**
     * Execute
     * ssh ws64-2.dev.exasol.com -L 9083:vm031.cos.dev.exasol.com:9083 -L 8020:vm031.cos.dev.exasol.com:8020 -L 8888:vm031.cos.dev.exasol.com:8888
     * before running this test
     */
    @Test
    public void exportToTable() throws Exception {
        String dbname = "default";
        String table = "sample_07_parquet";
        String hiveMetastoreURL = "thrift://cloudera01.exacloud.de:9083";
        String hdfsURL = "hdfs://cloudera01.exacloud.de:8020/user/hive/warehouse/sample_07_parquet";
        HCatTableMetadata tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, dbname, table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        List<List<Object>> inputRows = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add("99-9999");
        row.add("Test Description");
        row.add(9999999);
        row.add(999999);
        inputRows.add(row);

        String hdfsUser = "hdfs";
        String file = "test.parq";
        HdfsSerDeExportService.exportToParquetTableTest(hdfsURL, hdfsUser, file, tableMeta, new ExaIteratorDummy(inputRows));
    }

}