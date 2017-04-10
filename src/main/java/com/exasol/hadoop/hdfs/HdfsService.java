package com.exasol.hadoop.hdfs;

import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatPartition;

import java.io.FileNotFoundException;
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
            final List<String> hdfsAddressesToUse,
            final String databaseName,
            final String tableName,
            final String hdfsAddress) throws Exception {
        
        UserGroupInformation ugi = useKerberos ? KerberosHadoopUtils.getKerberosUGI(kerberosCredentials) : UserGroupInformation.createRemoteUser(hdfsUser);
        List<String> tableInfo = ugi.doAs(new PrivilegedExceptionAction<List<String>>() {
            @Override
            public List<String> run() throws Exception {
                Configuration conf = new Configuration();
                if (useKerberos) {
                    conf.set("dfs.namenode.kerberos.principal", hdfsUser);
                }
                HiveConf hiveConf = new HiveConf(new Configuration(), HiveConf.class);
                hiveConf.set("hive.metastore.local", "false");
                hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hdfsAddress);
                hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
                if (useKerberos) {
                    hiveConf.set("hive.metastore.kerberos.principal", hdfsUser);
                    hiveConf.set("hive.metastore.sasl.enabled", "true");
                }

                // Get all directories (leafs only) of the table
                FileSystem realFs = getFileSystem(hdfsAddressesToUse, conf);
                FileSystemWrapper fs = new FileSystemWrapperImpl(realFs);
                List<String> partitionPaths = getPartitionPaths(tableRootPath, partitionColumns, MultiPartitionFilter.parseMultiFilterToStrings(partitionFilterSpec),databaseName,tableName,hiveConf);
                // Get all filenames for the table
                return getFilePaths(fs, partitionPaths);
            }
        });
        return tableInfo;
    }

    /**
     * Try creating a FileSystem for any of the provided hdfs urls
     */
    public static FileSystem getFileSystem(List<String> hdfsURLs, Configuration conf) throws IOException {
        Exception lastException = null;
        for (String hdfsURL : hdfsURLs) {
            try {
                System.out.println("Filesystem to connect to: " + hdfsURL);
                FileSystem realFs = FileSystem.get(new URI(hdfsURL), conf);
                // Dirty hack: The above creation of the filesystem does not actually connect to the hdfs namenode,
                // so it does not fail if the hostname is good but hdfs is not listening on the specified port.
                // So we need to do something (cheap) with the filesystem to check if it is really available.
                try {
                    realFs.getFileStatus(new Path("this_file_is_probably_not_existing"));
                }
                catch (FileNotFoundException e) { // Ignore
                }
                return realFs;
            }
            catch (Exception e) {
                lastException = e;
            }
        }
        throw new RuntimeException("None of the provided HDFS URLs is reachable: " + hdfsURLs.toString() + ". The error for the last URL '" + hdfsURLs.get(hdfsURLs.size()-1) + "' was: " + lastException.getClass().getName() + ": " + lastException.getMessage());
    }

    /**
     * @param rootPath Root directory of the table, e.g. "/user/hive/warehouse/albums_rc_multi_part"
     */
    static List<String> getPartitionPaths(String rootPath, List<HCatTableColumn> partitionColumns, List<String> filters,
                                          String databaseName,String tableName,HiveConf configuration) throws Exception {
        System.out.println("call getPartitionPaths() for rootPath=" + rootPath + " partitionColumns=" + partitionColumns.toString() + " filters: " + filters.toString());
        List<String> partitionPaths = new ArrayList<>();
        partitionPaths.add(rootPath);
        HCatClient hCatClient = HCatClient.create(configuration);
        List<HCatPartition> partitions = new ArrayList<>();
        if(filters.isEmpty()) {
            partitions = hCatClient.getPartitions(databaseName, tableName);
        }
        else{
            for(String filter : filters){
                partitions = hCatClient.listPartitionsByFilter(databaseName, tableName, filter);
            }
        }
        for (HCatPartition partition : partitions) {
            partitionPaths.add(partition.getLocation());
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
