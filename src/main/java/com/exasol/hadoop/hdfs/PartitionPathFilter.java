package com.exasol.hadoop.hdfs;

import com.exasol.hadoop.hcat.HCatTableColumn;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link org.apache.hadoop.fs.PathFilter} which applies a
 * partition filter and only returns the paths of partitions matching the
 * filter. If no filter is specified, all paths are returned.
 * 
 * Idea: We could retrieve all partitions (not the columns) from HCat and then only return those which are registered in HCat.
 * This way we return the same table than a query in Hive (which might return nothing although there are valid partition directories and data files in hdfs)
 */
public class PartitionPathFilter implements PathFilter {
    
    private final List<HCatTableColumn> partitionColumns;   // all partition columns of this table
    private final List<PartitionFilter> filters;
    private final int numPartitionsInPaths;    // the number of partitions the paths have. E.g. 2 for "/path/to/table/p_year=2014/p_month=01"

    PartitionPathFilter(List<HCatTableColumn> partitionColumns, List<PartitionFilter> filters, int numPartitionsInPaths) {
        this.partitionColumns = partitionColumns;
        this.filters = filters;
        this.numPartitionsInPaths = numPartitionsInPaths;
    }

    @Override
    public boolean accept(Path path) {
        if (path == null) {
            return false;
        }
        String name = path.getName();
        if (name == null || name.isEmpty()) {
            return false;
        }
        try {
            name = URLDecoder.decode(name, "UTF-8");    // Convert from URL encoding
        } catch(UnsupportedEncodingException ex) {
            // Ignore
            System.out.println("WARNING: Could not decode path from URL encoding: " + name);
        }
        Map<String, String> partitionValues;
        try {
            partitionValues = getPartitionValues(path);
            return MultiPartitionFilter.matchesAnyFilter(partitionValues, filters);
        } catch (Exception e) {
            System.out.println("WARNING: Ignored path " + path.toString() + " because of error " + e.toString());
            return false;
        }
    }
    
    /**
     * Extract the individual partition components of a path
     * For /path/to/table/p1=v1/p2=v2 we extract a map { "p1": "v1", "p2": "v2" }
     * In this example, numPartitionsInPaths should be 2
     */
    private Map<String, String> getPartitionValues(Path path) {
        Map<String, String> partitionValues = new HashMap<>();
        Path current = path;
        // Start with deepest folder and go up
        for (int i=0; i<numPartitionsInPaths; i++) {
            if (!current.getName().contains("=")) {
                throw new RuntimeException("WARNING: Ignoring path which is not a valid partition path: " + current.getName());
            }
            String[] keyVal = current.getName().split("=");
            // Check if the partition key is the expected one
            if (!keyVal[0].equals(partitionColumns.get(numPartitionsInPaths-i-1).getName())) {
                throw new RuntimeException("WARNING: Ignoring path because it does not fit into partition schema. current.getName(): " + current.getName() + " numPartitionsInPath: " + numPartitionsInPaths + " i: " + i + " partitionColumns: " + partitionColumns);
            }
            partitionValues.put(keyVal[0], keyVal[1]);
            if (i+1<numPartitionsInPaths) {    // Don't go up if we don't enter the loop again
                current = current.getParent();
            }
        }
        return partitionValues;
    }
}
