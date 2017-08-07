# Hadoop ETL UDFs

[![Build Status](https://travis-ci.org/EXASOL/hadoop-etl-udfs.svg?branch=master)](https://travis-ci.org/EXASOL/hadoop-etl-udfs)


###### Please note that this is an open source project which is officially supported by EXASOL. For any questions, you can contact our support team.

## Overview
Hadoop ETL UDFs are the main way to transfer data between EXASOL and Hadoop (HCatalog tables on HDFS). The SQL syntax for calling the UDFs is similar to that of EXASOL's native IMPORT and EXPORT commands, but with added UDF paramters for specifying the various necessary and optional Hadoop properties.

A brief overview of features includes support for:
* HCatalog Metadata (e.g., table location, columns, partitions).
* Multiple file formats (e.g., Parquet, ORC, RCFile)
* HDFS HA
* Partitions
* Parallelization

For a more detailed description of the features, please refer to the IMPORT and EXPORT sections below.

## Getting Started

Before you can start using the Hadoop ETL UDFs, you have to deploy the UDFs in your EXASOL database.
Please follow the [step-by-step deployment guide](doc/deployment-guide.md).

## Using the UDFs

After deloying the UDFs, you can begin using them to easily transfer data to and from Hadoop.

### IMPORT

The IMPORT UDFs load data into EXASOL from Hadoop (HCatalog tables on HDFS). To import data, you just need to execute the SQL statement ```IMPORT INTO ... FROM SCRIPT ETL.IMPORT_HCAT_TABLE WITH ...``` with the appropriate parameters. This calls the ```ETL.IMPORT_HCAT_TABLE``` UDF, which was previously created during deployment.

For example, run the following statement to import data into an existing table.
```sql
CREATE TABLE sample_07 (code VARCHAR(1000), description VARCHAR (1000), total_emp INT, salary INT);

IMPORT INTO sample_07
FROM SCRIPT ETL.IMPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'sample_07'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs';
```

Please see [IMPORT details](doc/import.md) for a full description.

### EXPORT

The EXPORT UDFs load data from EXASOL into Hadoop (HCatalog tables on HDFS). To export data, you just need to execute the SQL statement ```EXPORT ... INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH ...``` with the appropriate parameters. This calls the ```ETL.EXPORT_HCAT_TABLE``` UDF, which was previously created during deployment.

For example, run the following statement to export data from an existing table.
```sql
CREATE TABLE TABLE1 (COL1 SMALLINT, COL2 INT, COL3 VARCHAR(50));

EXPORT TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs';
```

Please see the [EXPORT details](doc/export.md) for a full description.
