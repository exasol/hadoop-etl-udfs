package com.exasol.hadoop.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility Methods to work with a multi partition filter, i.e. user specified multiple filters (logically connected by OR)
 */
public class MultiPartitionFilter {
    
    public static List<PartitionFilter> parseMultiFilter(String filter) {
        List<PartitionFilter> partitionFilters = new ArrayList<>();
        if (filter == null || filter.isEmpty()) {
            return partitionFilters;
        }
        String[] filterSpecs = filter.split(",");
        for (String filterSpec : filterSpecs) {
            String[] filterParts = filterSpec.split("/");
            Map<String, String> partitionFilter = new HashMap<>();
            for (String filterPart : filterParts) {
                String[] keyValue = filterPart.trim().split("=");
                partitionFilter.put(keyValue[0], keyValue[1]);
            }
            partitionFilters.add(new PartitionFilter(partitionFilter));
        }
        return partitionFilters;
    }
    
    public static boolean matchesAnyFilter(Map<String, String> partitionValues, List<PartitionFilter> filters) {
        if (filters.isEmpty()) {
            return true;
        }
        for (PartitionFilter filter : filters) {
            if (matchesSingleFilter(partitionValues, filter)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesSingleFilter(Map<String, String> partitionValues, PartitionFilter filter) {
        // Does not match if any partition is mentioned in the filter with a different value
        for (String partition : partitionValues.keySet()) {
            if (filter.containsPartition(partition) && !filter.getPartitionValue(partition).equals(partitionValues.get(partition))) {
                return false;
            }
        }
        return true;
    }
}
