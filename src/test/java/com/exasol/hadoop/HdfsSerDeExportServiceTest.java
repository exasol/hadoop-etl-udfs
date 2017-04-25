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
        // Obtain table metadata
        // albums_rc
        //String table = "albums_rc"; // works to a certain point
        String table = "artists_rc"; // works to a certain point
        //String table = "albums";
        String hiveMetastoreURL = "thrift://localhost:9083";
        //String hdfsURL = "hdfs://localhost:50070";
        String hdfsURL = "hdfs://localhost:8020/user/hive/warehouse/albums_rc";
        HCatTableMetadata tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, "default", table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        List<List<Object>> inputRows = new ArrayList<>();
        List<Object> row1 = new ArrayList<>();
        row1.add(1);  // artistid
        row1.add("artistname"); // artistname
        row1.add(2001);  // year
        inputRows.add(row1);

        HdfsSerDeExportService.exportToTableTest(hdfsURL, "hdfs", "dummyfile3", tableMeta, new ExaIteratorDummy(inputRows));
    }

}