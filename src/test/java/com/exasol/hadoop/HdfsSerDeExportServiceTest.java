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
        String hiveMetastoreURL = "thrift://localhost:9083";
        //String table = "albums";
        HCatTableMetadata tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, "default", table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        HdfsSerDeExportService.exportToTable("hdfs://localhost:50070", "hdfs", "dummyfile2", tableMeta);
    }

}