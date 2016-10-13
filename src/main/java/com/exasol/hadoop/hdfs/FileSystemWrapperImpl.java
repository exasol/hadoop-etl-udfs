package com.exasol.hadoop.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * FileSystemWrapper implementation backed by a "real" Hadoop FileSystem.
 */
public class FileSystemWrapperImpl implements FileSystemWrapper {
    
    private FileSystem fs;
    
    public FileSystemWrapperImpl(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public FileStatus[] listStatus(Path path, PathFilter filter) throws IOException {
      return fs.listStatus(path, filter);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return fs.listStatus(path);
    }
}
