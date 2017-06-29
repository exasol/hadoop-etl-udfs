# Hive Adapter for Virtual Schemas

## Overview

The Hive adapter for virtual schemas allows you to connect to the HDFS layer of Hadoop. It uses the [Hadoop ETL UDFs](https://github.com/natasha-pel/hadoop-etl-udfs) 
behind the scenes to obtain the requested data, when running a query on a virtual table. 
The implementation of the Hive adapter handles three major aspects:
•	How to map the tables in the source systems to virtual tables in EXASOL, including how to map the data types to EXASOL data types.
•	How is the SQL syntax of the data source, including identifier quoting, case-sensitivity, function names, or special syntax like LIMIT/TOP.
•	Which capabilities are supported by the data source. E.g. is it supported to run filters, to specify select list expressions, to run aggregation or scalar functions or to order or limit the result.
If you are interested in an introduction to virtual schemas please refer to the EXASOL user manual. You can find it in the [download area of the EXASOL user portal](https://www.exasol.com/portal/display/DOWNLOAD/6.0).


## Getting Started

Before you can start using the Hive adapter for virtual schemas you have to deploy the Hadoop-etl-udfs in your EXASOL database.
Please follow the [step-by-step deployment guide](https://github.com/EXASOL/hadoop-etl-udfs/blob/master/doc/deployment-guide.md).

## Creating the Adapter

You create the Hive adapter with the following command:

```sql
CREATE JAVA  ADAPTER SCRIPT "HIVE_ADAPTER" AS
%scriptclass com.exasol.adapter.HiveAdapter;

  %jar /buckets/bucketfs1/bucket1/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar;

```

## Using the Adapter

The following statements demonstrate how you can use virtual schemas with the Hive  adapter to connect to a Hive system. Please scroll down to see a list of all properties supported by the Hive adapter.
First we create a virtual schema using the Hive adapter. The adapter will retrieve the metadata via HCatalog and map them to virtual tables. The metadata (virtual tables, columns and data types) are then cached in EXASOL.

```sql
CREATE VIRTUAL SCHEMA HIVE_SCHEMA
 USING HIVE_ADAPTER WITH 
HCAT_DB='xperience'
HCAT_ADDRESS='thrift://cloudera01.exacloud.de:9083'
HDFS_USER='hdfs';
```

We can now explore the tables in the virtual schema, just like for a regular schema:
```sql
OPEN SCHEMA HIVE_SCHEMA;
SELECT * FROM cat;
DESCRIBE sample_07;
```

And we can run arbitrary queries on the virtual tables:
```sql
SELECT count(*) FROM sample_07;
SELECT DISTINCT CODE FROM sample_07;
```

Behind the scenes the EXASOL command IMPORT INTO(…) FROM SCRIPT ETL.IMPORT_HCAT_TABLE will be executed to obtain the data needed from the data source to fulfil the query.
The EXASOL database interacts with the adapter to pushdown as much as possible to the data source, while considering the capabilities of the data source.
You can refresh the schemas metadata, e.g. if tables were added in the remote system:
```sql
ALTER VIRTUAL SCHEMA hive REFRESH;
ALTER VIRTUAL SCHEMA hive REFRESH TABLES t1 t2; -- refresh only these tables
```

Or set properties. Depending on the adapter and the property you set this might update the metadata or not. In our example the metadata are affected, because afterwards the virtual schema will only expose two virtul tables.
```sql
ALTER VIRTUAL SCHEMA hive SET TABLE_FILTER='SAMPLE_07, ALL_HIVE_DATA_TYPES';
```

Finally you can unset properties:
```sql
ALTER VIRTUAL SCHEMA hive SET TABLE_FILTER=null;
```

Or drop the virtual schema:
```sql
DROP VIRTUAL SCHEMA hive CASCADE;
```

### Adapter Properties
The following properties can be used to control the behavior of the Hive adapter.
As you see above, these properties can be defined in ```CREATE VIRTUAL SCHEMA``` or changed afterwards via ```ALTER VIRTUAL SCHEMA SET```. 
Note that properties are always strings, like `HCAT_DB='T1,T2'`.

**Mandatory Properties:**

Parameter                   | Value
--------------------------- | -----------
**HCAT_DB**                 | HCatalog Database Name. E.g. ```'xperience'```
**HCAT_ADDRESS**            | (Web)HCatalog Address. E.g. 'thrift://hive-metastore-host:9083' if you want to use the Hive Metastore (recommended), or 'webhcat-host:50111' if you want to use WebHCatalog. Make sure EXASOL can connect to these services (see prerequisites above).
**HDFS_USER**               | Username for HDFS authentication. E.g. 'hdfs', or 'hdfs/_HOST@EXAMPLE.COM' for Kerberos (see Kerberos Authentication below).






