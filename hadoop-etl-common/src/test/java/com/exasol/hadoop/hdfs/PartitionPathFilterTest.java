package com.exasol.hadoop.hdfs;

import com.exasol.hadoop.hcat.HCatTableColumn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertEquals;

public class PartitionPathFilterTest {
    @Test
    public void testAccept() {
        int numberOfPartitions = 3;
        List<HCatTableColumn> partitionColumns = new ArrayList<>();

        partitionColumns.add(new HCatTableColumn("year", "int"));
        partitionColumns.add(new HCatTableColumn("month", "int"));
        partitionColumns.add(new HCatTableColumn("day", "int"));

        PartitionPathFilter partionPathFilter = new PartitionPathFilter(
                partitionColumns,
                new ArrayList<>(),
                numberOfPartitions
        );

        Path path = new Path("metric/year=2017/month=9/day=25");

        assertEquals(true, partionPathFilter.accept(path));
    }
}
