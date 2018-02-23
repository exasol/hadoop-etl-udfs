# Hadoop ETL UDF EXPORT

## Table of Contents
1. [Overview](#overview)
2. [Using the EXPORT ETL UDFs](#using-the-export-etl-udfs)
3. [Parameters](#parameters)
4. [Options](#options)
5. [Partitions](#partitions)
6. [Debugging](#debugging)

## Overview
Hadoop ETL UDFs are the main way to load data from EXASOL into Hadoop (HCatalog tables on HDFS).

The features in detail:
* Metadata are retrieved from HCatalog (table location, columns, partitions, etc.).
* Supports loading data into both static and dynamic partitions.
* Supports HDFS HA environments (see ```HDFS_URL``` parameter below)
* Parallel Transfer:
  * Data is loaded directly from the EXASOL nodes into the Hadoop data nodes.
  * Parallelization is applied if the data set is grouped (using GROUP BY).
  * Degree of parallelism can be controlled by the number of data set groups.
  
Current limitations:
* Supports only the Parquet file format.
* Supports only Parquet compression.
* Supports only non-complex data types.

## Using the EXPORT ETL UDFs

The following examples assume that you have simple authentication. If your Hadoop requires Kerberos authentication, please refer to the [Kerberos Authentication](deployment-guide.md#5-kerberos-authentication) section.

Exporting data using the ETL UDFs works in much the way as the normal EXPORT command.

The following examples assume that the following table exists in Exasol.
```sql
CREATE TABLE TABLE1 (COL1 SMALLINT, COL2 INT, COL3 VARCHAR(50));
```

Run the following query to EXPORT a table.
```sql
EXPORT TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HCAT_USER       = 'hive';
 HDFS_USER       = 'hdfs';
```

Run the following query to EXPORT selected columns of a table.
```sql
EXPORT TABLE1 (COL1, COL3)
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HCAT_USER       = 'hive';
 HDFS_USER       = 'hdfs';
```

Run the following query to EXPORT the result set of a query.
```sql
EXPORT (SELECT COL2, COL3 FROM TABLE1)
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HCAT_USER       = 'hive';
 HDFS_USER       = 'hdfs';
```
## Parameters

### Mandatory Parameters

Parameter           | Value
------------------- | -----------
**HCAT_DB**         | HCatalog Database Name. E.g. ```'default'```
**HCAT_TABLE**      | HCatalog Table Name. E.g. ```'sample_07'```.
**HCAT_ADDRESS**    | (Web)HCatalog Address. E.g. ```'thrift://hive-metastore-host:9083'``` if you want to use the Hive Metastore (recommended), or ```'webhcat-host:50111'``` if you want to use WebHCatalog. Make sure EXASOL can connect to these services (see prerequisites above).

### Authentication Parameters

Parameter           | Value
------------------- | -----------
**HDFS_USER**       | Username for HDFS authentication (only if Kerberos is not used). E.g. ```'hdfs'```.
**HCAT_USER**       | Username for HCatalog authentication (only if Kerberos is not used). E.g. ```'hive'```.
**AUTH_TYPE**       | The authentication type to be used. Specify ```'kerberos'``` (case insensitive) to use Kerberos. Otherwise, simple authentication will be used.
**KERBEROS_CONNECTION**               | The name of the connection to be used if Kerberos authentication is enabled. It contains the credentials (user principal, keytab and kerberos config file) for the user to be used for HCatalog and Hdfs.
**KERBEROS_HDFS_SERVICE_PRINCIPAL**   | Kerberos Service Principal for HDFS. E.g. ```'hdfs/_HOST@EXAMPLE.COM'```.
**KERBEROS_HCAT_SERVICE_PRINCIPAL**   | Kerberos Service Principal for HCatalog. E.g. ```'hive/_HOST@EXAMPLE.COM'```. Since HCatalog is access through Hive, typically the service principal of Hive must be specified.

### Optional Parameters

Parameter           | Value
------------------- | -----------
**HDFS_URL**        | One or more URLs for HDFS/WebHDFS/HttpFS. E.g. ```'hdfs://hdfs-namenode:8020'``` (native HDFS) or ```'webhdfs://hdfs-namenode:50070'``` (WebHDFS) ```'webhdfs://hdfs-namenode:14000'``` (HttpFS). If you do not set this parameter the HDFS URL will be retrieved from HCatalog, but you have to set this parameter to overwrite the retrieved valie in several cases: First, if you have an HDFS HA environment you have to specify all namenodes (comma separated). Second, if you want to use WebHDFS instead of the native HDFS interface. And third, if HCatalog returns a non fully-qualified HDFS hostname unreachable from EXASOL. Make sure EXASOL can connect to the specified HDFS service (see prerequisites above).
**ENABLE_RPC_ENCRYPTION**   |  Set to ```'true'```, if Hadoop RPC encryption is enabled. Default value is ```'false'```.
**STATIC_PARTITION**  | The partition into which the exported data should be written (e.g., ```'part1=2015-01-01/part2=EU'```). If the partition does not exist, it will be created.
**DYNAMIC_PARTITION_EXA_COLS**  | The names of the Exasol columns to be used as the table's partitions while loading the data using dynamic partitioning (e.g., ```'COL1/COL2'```). Multiple column names can be separated by ```/```. If any partitions do not exist, they will be created. If the table has partitions and neither ```STATIC_PARTITION``` nor ```DYNAMIC_PARTITION_EXA_COLS``` are specified, the last Exasol columns are used as the table's partitions.
**COMPRESSION_TYPE**        | The name of the compression codec to be used for file compression (e.g., ```'snappy'```). The default value is uncompressed.
**JDBC_AUTH_TYPE**       | The authentication type to be used for JDBC optional connections. Specify ```'kerberos'``` (case insensitive) to use Kerberos. Otherwise, user/password authentication will be used.
**JDBC_CONNECTION**        | The connection name to used for optional JDBC connections.

## Options

The following options may be used to create or modify the destination table before the EXPORT is started.

Option           | Action
------------------- | -----------
**REPLACE** | Drops the destination table before the EXPORT is started.
**TRUNCATE** | Truncates the destination table before the EXPORT is started.
**CREATED BY** | Creates the destination table using the specified string before the EXPORT is started.

Note: The SQL statements for these options are executed using the Apache Hive JDBC driver. Thus, in order to use them, the ```JDBC_CONNECTION``` paramater must be specified. If ```JDBC_CONNECTION``` is a Kerberos connection, ```JDBC_AUTH_TYPE``` must also be specified.

The following examples assume that the following connection exists in Exasol.
```sql
CREATE CONNECTION HIVE_JDBC_CONN TO 'jdbc:hive2://hive-host:10000/' USER 'hive-user' IDENTIFIED BY 'hive-password';
```

Create the destination table before starting the EXPORT.
```sql
EXPORT TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs';
 HCAT_USER       = 'hive';
 JDBC_CONNECTION = 'hive_jdbc_conn'
CREATED BY 'CREATE TABLE default.test_table(co1 INT, col2 TIMESTAMP) STORED AS PARQUET';
```

Truncate the destination table before starting the EXPORT.
```sql
EXPORT TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs';
 HCAT_USER       = 'hive';
 JDBC_CONNECTION = 'hive_jdbc_conn'
TRUNCATE;
```

Replace the destination table before starting the EXPORT.
```sql
EXPORT TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs';
 HCAT_USER       = 'hive';
 JDBC_CONNECTION = 'hive_jdbc_conn'
REPLACE
CREATED BY 'CREATE TABLE default.test_table(co1 INT, col2 TIMESTAMP) STORED AS PARQUET';
```

## Partitions

If the destination table has partitions, data can be exported into it in one of two ways: specifying a static partition or using dynamic partitioning.

### Specifying a Static Partition

If a static partition is specified using ```STATIC_PARTITION```, all of the exported data is written into that single partition. Note that all table partitions must be specified.

For example, assume that the following table exists in Exasol.
```sql
CREATE TABLE TABLE1 (YEAR INT, MONTH INT, TEST_DATA VARCHAR(50));
```

It can exported into a partitioned table by specifying a static partition using the following query.
```sql
EXPORT
TABLE1(TEST_DATA)
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB          = 'default'
 HCAT_TABLE       = 'test_table'
 HCAT_ADDRESS     = 'thrift://hive-metastore-host:9083'
 HDFS_USER        = 'hdfs';
 HCAT_USER        = 'hive';
 JDBC_CONNECTION  = 'hive_jdbc_conn'
 STATIC_PARTITION = 'year=2017/month=8'
CREATED BY 'CREATE TABLE default.test_table(data_col VARCHAR(200)) PARTITIONED BY (year INT, month INT) STORED AS PARQUET';
```
### Using Dynamic Partitioning

If a static partition is not specified, dynamic partitioning will be used to export data into partitioned tables. Dynamic partitioning uses the data values from the appropriate Exasol columns to automatically determine into which partition the data should be imported.

To specify which Exasol columns should be used for the destination table's partitions, the ```DYNAMIC_PARTITION_EXA_COLS``` parameter should be specified. This is done by listing the names of the Exasol columns, which correspond the destination table's partitions.

For example, assume that the following table exists in Exasol.
```sql
CREATE TABLE TABLE1 (YEAR INT, TEST_DATA VARCHAR(50), COUNTRY VARCHAR(50));
```

It can exported into a partitioned table by specifying the dynamic partition columns using the following query.
```sql
EXPORT
TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB                     = 'default'
 HCAT_TABLE                  = 'test_table'
 HCAT_ADDRESS                = 'thrift://hive-metastore-host:9083'
 HDFS_USER                   = 'hdfs';
 HCAT_USER                   = 'hive';
 JDBC_CONNECTION             = 'hive_jdbc_conn'
 DYNAMIC_PARTITION_EXA_COLS  = 'COUNTRY/YEAR'
CREATED BY 'CREATE TABLE default.test_table(data_col VARCHAR(200)) PARTITIONED BY (country VARCHAR(200), year INT) STORED AS PARQUET';
```

If the destination table has partitions and neither ```STATIC_PARTITION``` nor ```DYNAMIC_PARTITION_EXA_COLS``` are specified, dynamic partitioning will be assumed. In this case, the last X columns of data in Exasol will be used as the dynamic partitions, where X is the destination table's number of partitions.

In the following example, the last two Exasol columns are used for dynamic partitioning because the destination table has two partitions and neither ```STATIC_PARTITION``` nor ```DYNAMIC_PARTITION_EXA_COLS``` are specified.
```sql
EXPORT
TABLE1(TEST_DATA, COUNTRY, YEAR)
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs';
 HCAT_USER       = 'hive';
 JDBC_CONNECTION = 'hive_jdbc_conn'
CREATED BY 'CREATE TABLE default.test_table(data_col VARCHAR(200)) PARTITIONED BY (country VARCHAR(200), year INT) STORED AS PARQUET';
```

## Debugging
To see debug output relating to Hadoop and the UDFs, you can use the Python script udf_debug.py located in the [tools](../tools) directory.

First, start the udf_debug.py script, which will listen on the specified address and port and print all incoming text.
```
python tools/udf_debug.py -s myhost -p 3000
```
Then run the following SQL statement in your session to redirect all stdout and stderr from the adapter script to the udf_debug.py script we started before.
```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS='host-where-udf-debug-script-runs:3000';
```
