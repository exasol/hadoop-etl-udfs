package com.exasol.hadoop.hdfs;

import java.util.Map;
import java.util.Set;

public class PartitionFilter {
    private Map<String, String> filter;
    
    public PartitionFilter(Map<String, String> filter) {
        this.filter = filter;
    }
    
    public boolean containsPartition(String name) {
        return filter.containsKey(name);
    }
    
    public String getPartitionValue(String partitionName) {
        return filter.get(partitionName);
    }
    
    public Set<String> getPartitions() {
        return filter.keySet();
    }
}
