package com.exasol.hadoop.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * Wraps {@link org.apache.hadoop.fs.FileSystem} to make it testable
 */
public interface FileSystemWrapper {
    FileStatus[] listStatus(Path path, PathFilter filter) throws IOException;
    FileStatus[] listStatus(Path path) throws IOException;
}
