package com.exasol.hadoop;

import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hive.HiveMetastoreService;
import org.junit.Test;

public class HdfsSerDeExportServiceTest {

    @Test
    public void exportToTable() throws Exception {
        // Obtain table metadata
        // albums_rc
        String table = "albums_rc"; // works to a certain point
        //String table = "albums";
        String hiveMetastoreURL = "thrift://localhost:9083";
        //String hdfsURL = "hdfs://localhost:50070";
        String hdfsURL = "hdfs://localhost:8020/user/hive/warehouse/albums_rc";
        HCatTableMetadata tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, "default", table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        HdfsSerDeExportService.exportToTableTest(hdfsURL, "hdfs", "dummyfile3", tableMeta);
    }

}