# Hadoop ETL UDFs 1.1.0, released 2022-02-15

Code name: Updated Kerberos `auth_to_local` mechanism to use MIT

## Summary

In this release, we have updated Kerberos [`auth_to_local`](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html) mechanism to use MIT configuration file. Additionally, we updated vulnerable dependencies and improved documentation.

## Bug Fixes

* #73: Updated vulnerable Hadoop dependencies

## Features

* #74: Updated Kerberos `auth_to_local` mechanism property to MIT

## Documentation

* #60: Increased the file length in deployment script
* #69: Added documentation on building for Cloudera 6.x and above versions

## Features

## Dependency Updates

### Compile Dependency Updates

* Updated `org.apache.hadoop:hadoop-annotations:2.8.5` to `2.10.1`
* Updated `org.apache.hadoop:hadoop-common:2.8.5` to `2.10.1`
* Updated `org.apache.hadoop:hadoop-hdfs:2.8.5` to `2.10.1`
* Updated `org.apache.hadoop:hadoop-mapreduce-client-core:2.8.5` to `2.10.1`
* Updated `org.apache.hadoop:hadoop-mapreduce-client-common:2.8.5` to `2.10.1`

### Test Dependency Updates

### Plugin Updates
