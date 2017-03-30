package com.exasol.hadoop.hdfs;

import com.exasol.hadoop.hcat.HCatTableColumn;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class HCatMetadataServiceTest {
    
    private static class FakeFileSystem implements FileSystemWrapper {
        
        private Map<String, List<FileStatus>> pathsAndContent;
        
        public FakeFileSystem(Map<String, List<FileStatus>> pathsAndContent) {
            this.pathsAndContent = pathsAndContent;
        }

        @Override
        public FileStatus[] listStatus(Path path, PathFilter filter) throws IOException {
            List<FileStatus> subPaths = pathsAndContent.get(path.toString());
            List<FileStatus> filteredSubPaths = new ArrayList<>();
            for (FileStatus subPath : subPaths) {
                if (filter.accept(subPath.getPath())) {
                    filteredSubPaths.add(fakeFileStatus(subPath.getPath().toString()));
                }
            }
            return filteredSubPaths.toArray(new FileStatus[filteredSubPaths.size()]);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            throw new RuntimeException("Not implemented in fake object");
        }
    }
    
//    @Test
//    public void testGetPartitionPaths() throws Exception {
//
//        List<HCatTableColumn> partitionColumns = new ArrayList<>();
//        partitionColumns.add(new HCatTableColumn("y", "ignore"));
//        partitionColumns.add(new HCatTableColumn("m", "ignore"));
//
//        Map<String, List<FileStatus>> pathsAndContent = new HashMap<>();
//        String rootPath = "/user/hive/warehouse/albums_rc_multi_part";
//        pathsAndContent.put(
//                rootPath,
//                Arrays.asList(
//                        fakeFileStatus(rootPath + "/y=2001"),
//                        fakeFileStatus(rootPath + "/y=2002"),
//                        fakeFileStatus(rootPath + "/y=2003")));
//
//        pathsAndContent.put(
//                rootPath + "/y=2001",
//                Arrays.asList(
//                        fakeFileStatus(rootPath + "/y=2001/m=01"),
//                        fakeFileStatus(rootPath + "/y=2001/m=02")));
//
//        pathsAndContent.put(
//                rootPath + "/y=2002",
//                Arrays.asList(
//                        fakeFileStatus(rootPath + "/y=2002/m=01"),
//                        fakeFileStatus(rootPath + "/y=2002/m=02"),
//                        fakeFileStatus(rootPath + "/y=2002/m=03")));
//
//        pathsAndContent.put(
//                rootPath + "/y=2003",
//                Arrays.asList(
//                        fakeFileStatus(rootPath + "/y=2003/m=01"),
//                        fakeFileStatus(rootPath + "/y=2003/m=11")));
//
//        String filter = "y=2001/m=01";
//        List<String> expectedPaths = Arrays.asList(
//                rootPath + "/y=2001/m=01");
//        testGetPartitionPathsScenario(partitionColumns, pathsAndContent, rootPath, filter, expectedPaths);
//
//        filter = "y=2003";
//        expectedPaths = Arrays.asList(
//                rootPath + "/y=2003/m=01",
//                rootPath + "/y=2003/m=11");
//        testGetPartitionPathsScenario(partitionColumns, pathsAndContent, rootPath, filter, expectedPaths);
//
//        filter = "y=2001/m=01, y=2002/m=02, y=2003";
//        expectedPaths = Arrays.asList(
//                rootPath + "/y=2001/m=01",
//                rootPath + "/y=2002/m=02",
//                rootPath + "/y=2003/m=01",
//                rootPath + "/y=2003/m=11");
//        testGetPartitionPathsScenario(partitionColumns, pathsAndContent, rootPath, filter, expectedPaths);
//
//        // No filter
//        filter = "";
//        expectedPaths = Arrays.asList(
//                rootPath + "/y=2001/m=01",
//                rootPath + "/y=2001/m=02",
//                rootPath + "/y=2002/m=01",
//                rootPath + "/y=2002/m=02",
//                rootPath + "/y=2002/m=03",
//                rootPath + "/y=2003/m=01",
//                rootPath + "/y=2003/m=11");
//        testGetPartitionPathsScenario(partitionColumns, pathsAndContent, rootPath, filter, expectedPaths);
//    }
//
//    private void testGetPartitionPathsScenario(List<HCatTableColumn> partitionColumns, Map<String, List<FileStatus>> pathsAndContent, String rootPath, String filter,
//            List<String> expectedPaths) throws Exception {
//        FakeFileSystem fs = new FakeFileSystem(pathsAndContent);
//        List<String> paths = HdfsService.getPartitionPaths(fs, rootPath, partitionColumns, MultiPartitionFilter.parseMultiFilter(filter));
//        assertEquals(expectedPaths, paths);
//    }
    
    private static FileStatus fakeFileStatus(String path) {
        return new FileStatus(0, true, 0, 0, 0, new Path(path));
    }

    @Test
    public void testFilterMatcherSingle() {
        List<PartitionFilter> filter = MultiPartitionFilter.parseMultiFilter("y=2001/m=01");
        
        // Less partitions than specified in filter
        Map<String, String> partition = new HashMap<>();
        partition.put("y", "2001");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));
        
        partition = new HashMap<>();
        partition.put("y", "2002");
        assertEquals(false, MultiPartitionFilter.matchesAnyFilter(partition, filter));
        
        // Exact number of partitions
        partition = new HashMap<>();
        partition.put("y", "2001");
        partition.put("m", "01");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));

        partition = new HashMap<>();
        partition.put("y", "2001");
        partition.put("m", "02");
        assertEquals(false, MultiPartitionFilter.matchesAnyFilter(partition, filter));

        partition = new HashMap<>();
        partition.put("y", "2002");
        partition.put("m", "01");
        assertEquals(false, MultiPartitionFilter.matchesAnyFilter(partition, filter));
        
        // More partitions (some are not specified in filter)
        partition = new HashMap<>();
        partition.put("y", "2001");
        partition.put("m", "01");
        partition.put("d", "01");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));
    }

    @Test
    public void testFilterMatcherMulti() {
        List<PartitionFilter> filter = MultiPartitionFilter.parseMultiFilter("y=2001/m=01, y=2002/m=02, y=2003");
        
        Map<String, String> partition = new HashMap<>();
        partition.put("y", "2003");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));
        
        partition = new HashMap<>();
        partition.put("y", "2004");
        assertEquals(false, MultiPartitionFilter.matchesAnyFilter(partition, filter));
        
        partition = new HashMap<>();
        partition.put("y", "2001");
        partition.put("m", "01");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));

        partition = new HashMap<>();
        partition.put("y", "2001");
        partition.put("m", "02");
        assertEquals(false, MultiPartitionFilter.matchesAnyFilter(partition, filter));

        partition = new HashMap<>();
        partition.put("y", "2003");
        partition.put("m", "01");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));
        
        partition = new HashMap<>();
        partition.put("y", "2001");
        partition.put("m", "01");
        partition.put("d", "01");
        assertEquals(true, MultiPartitionFilter.matchesAnyFilter(partition, filter));
    }
    
    @Test
    public void testPartitionPathFilter() {
        // Specify all folders, as they would be returned by a real file system
        List<String> pathsPartition1 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000",
                "/user/hive/warehouse/albums_rc_multi_part/y=2013",
                "/user/hive/warehouse/albums_rc_multi_part/y=2014",
                "/user/hive/warehouse/albums_rc_multi_part/y=2015");
        
        List<String> pathsPartition2 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2010/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/y=2013/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/y=2013/m=02",
                "/user/hive/warehouse/albums_rc_multi_part/y=2014/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/y=2014/m=02",
                "/user/hive/warehouse/albums_rc_multi_part/y=2014/m=03",
                "/user/hive/warehouse/albums_rc_multi_part/y=2015/m=11",
                "/user/hive/warehouse/albums_rc_multi_part/y=2015/m=12");
        
        List<HCatTableColumn> partitionColumns = Arrays.asList(
                new HCatTableColumn("y", "int"),
                new HCatTableColumn("m", "int")
                );
        
        // Filter, and check result
        List<PartitionFilter> multiFilter = MultiPartitionFilter.parseMultiFilter("y=2013/m=01, y=2014/m=02, y=2015");
        List<String> filteredPaths1 = fakeFilterPaths(pathsPartition1, new PartitionPathFilter(partitionColumns, multiFilter, 1));
        List<String> expectedFilterd1 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2013",
                "/user/hive/warehouse/albums_rc_multi_part/y=2014",
                "/user/hive/warehouse/albums_rc_multi_part/y=2015");
        assertEquals(expectedFilterd1, filteredPaths1);
        List<String> filteredPaths2 = fakeFilterPaths(pathsPartition2, new PartitionPathFilter(partitionColumns, multiFilter, 2));
        List<String> expectedFilterd2 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2013/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/y=2014/m=02",
                "/user/hive/warehouse/albums_rc_multi_part/y=2015/m=11",
                "/user/hive/warehouse/albums_rc_multi_part/y=2015/m=12");
        assertEquals(expectedFilterd2, filteredPaths2);
        
        // Test with a single filter
        multiFilter = MultiPartitionFilter.parseMultiFilter("y=2013/m=01");
        List<String> filteredPaths3 = fakeFilterPaths(pathsPartition2, new PartitionPathFilter(partitionColumns, multiFilter, 2));
        List<String> expectedFilterd3 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2013/m=01");
        assertEquals(expectedFilterd3, filteredPaths3);
    }
    
    @Test
    public void testPartitionPathFilterInvalidDirs() {
        // Specify all folders, as they would be returned by a real file system
        List<String> pathsPartition1 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000",
                "/user/hive/warehouse/albums_rc_multi_part/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/INVALID",
                "/user/hive/warehouse/albums_rc_multi_part/INVALID=2001");
        
        List<String> pathsPartition2 = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/m=02",
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/y=2000",
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/INVALID",
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/INVALID=03");
        
        List<HCatTableColumn> partitionColumns = Arrays.asList(
                new HCatTableColumn("y", "int"),
                new HCatTableColumn("m", "int")
                );
        
        // No filter
        List<PartitionFilter> multiFilter = MultiPartitionFilter.parseMultiFilter("");
        List<String> filteredPaths = fakeFilterPaths(pathsPartition1, new PartitionPathFilter(partitionColumns, multiFilter, 1));
        List<String> expectedFilterd = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000");
        assertEquals(expectedFilterd, filteredPaths);
        filteredPaths = fakeFilterPaths(pathsPartition2, new PartitionPathFilter(partitionColumns, multiFilter, 2));
        expectedFilterd = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/m=01",
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/m=02");
        assertEquals(expectedFilterd, filteredPaths);
        
        // Filter not matching anything
        multiFilter = MultiPartitionFilter.parseMultiFilter("y=2001/m=01");
        filteredPaths = fakeFilterPaths(pathsPartition1, new PartitionPathFilter(partitionColumns, multiFilter, 1));
        expectedFilterd = Arrays.asList();
        assertEquals(expectedFilterd, filteredPaths);

        // Filter matching a single dir
        multiFilter = MultiPartitionFilter.parseMultiFilter("y=2000/m=02");
        filteredPaths = fakeFilterPaths(pathsPartition1, new PartitionPathFilter(partitionColumns, multiFilter, 1));
        expectedFilterd = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000");
        assertEquals(expectedFilterd, filteredPaths);
        filteredPaths = fakeFilterPaths(pathsPartition2, new PartitionPathFilter(partitionColumns, multiFilter, 2));
        expectedFilterd = Arrays.asList(
                "/user/hive/warehouse/albums_rc_multi_part/y=2000/m=02");
        assertEquals(expectedFilterd, filteredPaths);
    }

    private static List<String> fakeFilterPaths(List<String> paths, PartitionPathFilter pathFilter) {
        List<String> filteredPaths = new ArrayList<>();
        for (String path : paths) {
            if (pathFilter.accept(new Path(path))) {
                filteredPaths.add(path);
            }
        }
        return filteredPaths;
    }

    @Test
    public void testParseMultiPartitionFilter() {
        String partitionFilter = "y=2013/m=01, y=2014/m=02, y=2015";
        List<PartitionFilter> filters = MultiPartitionFilter.parseMultiFilter(partitionFilter);
        
        assertEquals(3, filters.size());
        assertEquals(2, filters.get(0).getPartitions().size());
        assertEquals("2013", filters.get(0).getPartitionValue("y"));
        assertEquals("01", filters.get(0).getPartitionValue("m"));
        assertEquals(2, filters.get(1).getPartitions().size());
        assertEquals("2014", filters.get(1).getPartitionValue("y"));
        assertEquals("02", filters.get(1).getPartitionValue("m"));
        assertEquals(1, filters.get(2).getPartitions().size());
        assertEquals("2015", filters.get(2).getPartitionValue("y"));
    }
    
}
