# Hadoop ETL UDF IMPORT

## Table of Contents
1. [Overview](#overview)
2. [Using the IMPORT ETL UDFs](#using-the-import-etl-udfs)
3. [Parameters](#parameters)
4. [Performance](#performance)
5. [Debugging](#debugging)

## Overview
Hadoop ETL UDFs are the main way to load data into EXASOL from Hadoop (HCatalog tables on HDFS).

The features in detail:
* Metadata are retrieved from HCatalog (HDFS files, file formats, columns, etc.).
* Supports all Hive SerDes (Parquet, ORC, RC, Avro, JSON, etc.).
* Supports compression for SerDe (e.g., ORC compression) and for Hive (```hive.exec.compress.*```).
* Supports complex data types (array, map, struct, union) and JsonPath. Values of complex data types are returned as JSON. You can also specify simple JSONPath expressions.
* Supports to specify filters which partitions to load.
* Supports HDFS HA environments (see ```HDFS_URL``` parameter below)
* Parallel Transfer:
  * Data is loaded directly from the data node to one of the EXASOL nodes.
  * Parallelization is applied if the HCatalog table consists of multiple files.
  * Degree of parallelism can be controlled via an UDF parameter. The maximum degree is determined by the number of HDFS files and the number of EXASOL nodes and cores.

## Using the IMPORT ETL UDFs

The following examples assume that you have simple authentication. If your Hadoop requires Kerberos authentication, please refer to the [Kerberos Authentication](kerberos.md) documentation.

Run the following query to show the contents of the HCatalog table sample_07.
```sql
IMPORT INTO (code VARCHAR(1000), description VARCHAR (1000), total_emp INT, salary INT)
FROM SCRIPT ETL.IMPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'sample_07'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HCAT_USER       = 'hive'
 HDFS_USER       = 'hdfs';
```

Run the following statement to import into an existing table.
```sql
CREATE TABLE sample_07 (code VARCHAR(1000), description VARCHAR (1000), total_emp INT, salary INT);

IMPORT INTO sample_07
FROM SCRIPT ETL.IMPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'sample_07'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HCAT_USER       = 'hive'
 HDFS_USER       = 'hdfs';
```
The EMITS specification is not required because the columns are inferred from the target table.

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
**KERBEROS_CONNECTION**        | The name of the connection to be used if Kerberos authentication is enabled. It contains the credentials (user principal, keytab and kerberos config file) for the user to be used for HCatalog and Hdfs.
**KERBEROS_HDFS_SERVICE_PRINCIPAL**       | Kerberos Service Principal for HDFS. E.g. ```'hdfs/_HOST@EXAMPLE.COM'```.
**KERBEROS_HCAT_SERVICE_PRINCIPAL**       | Kerberos Service Principal for HCatalog. E.g. ```'hive/_HOST@EXAMPLE.COM'```. Since HCatalog is access through Hive, typically the service principal of Hive must be specified.

### Optional Parameters

Parameter           | Value
------------------- | -----------
**ENABLE_RPC_ENCRYPTION**   |  Set to ```'true'```, if Hadoop RPC encryption is enabled. Default value is ```'false'```.
**PARALLELISM**     | Degree of Parallelism, i.e. the maximum number of parallel JVM instances to be started for loading data. The default value ```'nproc()'```, which is the total number of nodes in the EXASOL cluster, will start one importing UDF on each node. We also recommend testing with larger values for better performance, e.g. ```'nproc() * 8'```. In our experiences, a factor significantly higher than the number of cores will not further improve performance.
**PARTITIONS**      | Partition Filter. E.g. ```'part1=2015-01-01/part2=EU'```. This parameter specifies which partitions should be loaded. For example, ```'part1=2015-01-01'``` will only load data with value ```2015-01-01``` for the partition ```part1```. Multiple partitions can be separated by ```/```. You can specify multiple comma-separated filters, e.g. ```'part1=2015-01-01/part2=EU, part1=2015-01-01/part2=UK'```. The default value ```''``` means all partitions should be loaded. Multiple values for a single partition are not supported(e.g. ```'part1=2015-01-01/part1=2015-01-02'```).
**OUTPUT_COLUMNS**  | Specification of the desired columns to output, e.g. ```'col1, col2.field1, col3.field1[0]'```. Supports simple [JsonPath](http://goessner.net/articles/JsonPath/) expressions: 1. dot operator, to access fields in a struct or map data type and 2. subscript operator (brackets) to access elements in an array data type. The JsonPath expressions can be arbitrarily nested.
**HDFS_URL**        | One or more URLs for HDFS/WebHDFS/HttpFS. E.g. ```'hdfs://hdfs-namenode:8020'``` (native HDFS) or ```'webhdfs://hdfs-namenode:50070'``` (WebHDFS) ```'webhdfs://hdfs-namenode:14000'``` (HttpFS). If you do not set this parameter the HDFS URL will be retrieved from HCatalog, but you have to set this parameter to overwrite the retrieved valie in several cases: First, if you have an HDFS HA environment you have to specify all namenodes (comma separated). Second, if you want to use WebHDFS instead of the native HDFS interface. And third, if HCatalog returns a non fully-qualified HDFS hostname unreachable from EXASOL. Make sure EXASOL can connect to the specified HDFS service (see prerequisites above).
**SHOW_SQL**        | Show the SQL which will be executed internally (for debugging purposes). This will not actually run the import.

## Performance
The actual performance is depending on too many factors, such as the Hadoop version (we use the Hadoop Java libraries for loading and deserializing, which might greatly vary in different versions), the performance and size of the Exasol and Hadoop cluster, network, etc. For this reason we recommend making your own tests and play with the parameters. Please consider following things which all have an impact on performance:
* Adjust the ```PARALLELISM``` parameter to increase the number of importing processes running at one one on each node (see table above).
* The parallelization works over files, i.e. the total parallelism is limited by the number of files. Make sure you have enough files.
* We recommend using Thrift for HDFS and HCatalog instead webHDFS or webHCAT, since the web-variants mean that data are streamed through an additional service on the Hadoop side which decreases performance and reliability.
* If you want to load only a portion of the table, it is the fastest to use the ```PARTITIONS``` parameter to load only a subset of all partitions (see table above).
* Using compression helps to save bandwidth, but slightly increases the time for deserialization in Exasol. Overall compression often improves performance.
* If your data are already stored in csv in HDFS, you can consider loading directly with ```IMPORT FROM CSV```, which will be faster compared to the Hadoop ETL UDFs. However, you will have to specify the files manually.

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
