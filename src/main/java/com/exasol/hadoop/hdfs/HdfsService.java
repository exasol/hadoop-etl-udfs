package com.exasol.hadoop.hdfs;

import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

public class HdfsService {
    
    public static List<String> getFilesFromTable(
            final String hdfsUser,
            final String tableRootPath,
            final String partitionFilterSpec,
            final List<HCatTableColumn> partitionColumns,
            final boolean useKerberos,
            final KerberosCredentials kerberosCredentials,
            final String hdfsAddressToUse) throws Exception {
        
        UserGroupInformation ugi = useKerberos ?
                KerberosHadoopUtils.getKerberosUGI(kerberosCredentials) : UserGroupInformation.createRemoteUser(hdfsUser);
        List<String> tableInfo = ugi.doAs(new PrivilegedExceptionAction<List<String>>() {
            @Override
            public List<String> run() throws Exception {
                System.out.println("Filesystem to connect to: " + hdfsAddressToUse);
                Configuration conf = new Configuration();
                if (useKerberos) {
                    conf.set("dfs.namenode.kerberos.principal", hdfsUser);
                }
                // Get all directories (leafs only) of the table
                FileSystemWrapper fs = new FileSystemWrapperImpl(FileSystem.get(new URI(hdfsAddressToUse), conf));
                List<String> partitionPaths = getPartitionPaths(fs, tableRootPath, partitionColumns, MultiPartitionFilter.parseMultiFilter(partitionFilterSpec));
                // Get all filenames for the table
                return getFilePaths(fs, partitionPaths);
            }
        });
        return tableInfo;
    }

    /**
     * @param rootPath Root directory of the table, e.g. "/user/hive/warehouse/albums_rc_multi_part"
     */
    static List<String> getPartitionPaths(FileSystemWrapper fs, String rootPath, List<HCatTableColumn> partitionColumns, List<PartitionFilter> filters) throws Exception {
        System.out.println("call getPartitionPaths() for rootPath=" + rootPath + " partitionColumns=" + partitionColumns.toString() + " filters: " + filters.toString());
        List<String> partitionPaths = new ArrayList<>(1); // Will hold all directories which are part of the filter (only leave-directories at the end)
        partitionPaths.add(rootPath);
        // If we have n partition columns, we need to go down in directory tree n times.
        for (int partition=0; partition<partitionColumns.size(); partition++) {
            // Populate new list containing all subdirectories all the current list of directories
            List<String> partPaths = new ArrayList<>();
            PartitionPathFilter pathFilter = new PartitionPathFilter(partitionColumns, filters, partition+1);
            for (String path : partitionPaths) {
                FileStatus[] fileStatuses = fs.listStatus(new Path(path), pathFilter);
                for (FileStatus stat : fileStatuses) {
                    partPaths.add(path + "/" + stat.getPath().getName());
                }
            }
            partitionPaths = partPaths;
        }
        return partitionPaths;
    }

    private static List<String> getFilePaths(FileSystemWrapper fs, List<String> partitionPaths) throws IOException {
        List<String> filePaths = new ArrayList<>();
        for (String path : partitionPaths) {
            FileStatus[] fileStatuses = fs.listStatus(new Path(path));
            for (FileStatus stat : fileStatuses) {
                if (!stat.isFile()) {
                    continue;
                }
                filePaths.add(path + "/" + stat.getPath().getName());
            }
        }
        return filePaths;
    }
    
}
