## Deploying the Hadoop ETL UDFs Step By Step

Run the following steps to deploy your UDFs:

### 1. Prerequisites:
* EXASOL Advanced Edition (version 6.0 or newer) or Free Small Business Edition.
* JDK & Maven to build from source
* Connectivity from EXASOL to Hadoop: Make sure that following Hadoop services can be accessed from EXASOL. In case of problems please use an [UDF to check the connectivity](https://www.exasol.com/support/browse/SOL-307).
  * All EXASOL nodes need access to either the Hive Metastore (recommended) or to WebHCatalog:
    * The Hive Metastore is typically running on port ```9083``` of the Hive Metastore server (```hive.metastore.uris``` property in Hive). It uses a native Thrift API, which is faster compared to WebHCatalog.
    * The WebHCatalog server (formerly called Templeton) is typically running on port ```50111``` on a certain server (```templeton.port``` property).
  * All EXASOL nodes need access to the namenode and all datanodes, either via the native HDFS interface or via the http REST API (WebHDFS or HttpFS)
    * HDFS (recommended): The namenode service typically runs on port ```8020``` (```fs.defaultFS``` property), the datanode service on port ```50010``` or ```1004``` in Kerberos environments (```dfs.datanode.address``` property).
    * WebHDFS: The namenode service for WebHDFS typically runs on port ```50070``` on each namenode (```dfs.namenode.http-address``` property), and on port ```50075``` (```dfs.datanode.http.address``` property) on each datanode. If you use https, the ports are ```50470``` for the namenode (```dfs.namenode.https-address```) and ```50475``` for the datanode (```dfs.datanode.https. address```).
    * HttpFS: Alternatively to WebHDFS you can use HttpFS, exposing the same REST API as WebHDFS. It typically runs on port ```14000``` of each namenode. The disadvantage compared to WebHDFS is that all data are streamed through a single service, whereas webHDFS redirects to the datanodes for the data transfer.
  * EXPORT Options: If you plan to use the EXPORT options which require ```JDBC_CONNECTION```, JDBC access from each EXASOL node to Hadoop must be provided. The JDBC driver usually connects to Hadoop using port ```10000```.
  * Kerberos: If your Hadoop uses Kerberos authentication, the UDFs will authenticate using a keytab file. Each EXASOL node needs access to the Kerberos KDC (key distribution center), running on port ```88```. The KDC is configured in the kerberos config file which is used for the authentication, as described in the [Kerberos Authentication](#5-kerberos-authentication) section.

### 2. Building from Source

First clone the repository on your computer.
```
git clone https://github.com/EXASOL/hadoop-etl-udfs.git
cd hadoop-etl-udfs
```

You have to build the sources depending on your Hive and Hadoop version as follows. The resulting fat JAR (including all dependencies) is stored in ```hadoop-etl-dist/target/hadoop-etl-dist-1.0.0-SNAPSHOT.jar```.

#### Cloudera CDH
You can look up the version numbers for Hadoop and Hive in the [CDH Maven documentation](https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_vd_cdh5_maven_repo.html) (search for the artifactId ```hadoop-common``` and ```hive-serde```).
```
mvn clean -DskipTests package -P cloudera -Dhadoop.version=2.6.0-cdh5.11.2 -Dhive.version=1.1.0-cdh5.11.2
```

#### Hortonworks HDP
You can look up the version numbers in the HDP release notes or use the command line:
```
[maria_dev@sandbox-hdp ~]$ hadoop version
Hadoop 2.7.3.2.6.3.0-235
[maria_dev@sandbox-hdp ~]$ hive --version
Hive 1.2.1000.2.6.3.0-235
```
Maven build command:
```
mvn clean -DskipTests package -P hortonworks -Dhadoop.version=2.7.3.2.6.3.0-235 -Dhive.version=1.2.1000.2.6.3.0-235
```

#### Other Hadoop Distributions
You may have to add a Maven repository to pom.xml for your distribution. Then you can compile similarly to examples above for other distributions.

#### Standard Apache Hadoop and Hive (no distribution)
```
mvn clean -DskipTests package assembly:single -P cloudera -Dhadoop.version=1.2.1 -Dhive.version=1.2.1
```
This command deactivates the Cloudera Maven profile which is active by default.

### 3. Upload Jar

You have to upload the jar to a bucket of your choice in the EXASOL bucket file system (BucketFS). This will allow using the jar in the UDF scripts.

Following steps are required to upload a file to a bucket:
* Make sure you have a bucket file system (BucketFS) and you know the port for either http or https. This can be done in EXAOperation under "EXABuckets". E.g. the id could be ```bucketfs1``` and the http port 2580.
* Check if you have a bucket in the BucketFS. Simply click on the name of the BucketFS in EXAOperation and add a bucket there, e.g. ```bucket1```. Also make sure you know the write password. For simplicity we assume that the bucket is defined as a public bucket, i.e. it can be read by any script.
* Now upload the file into this bucket, e.g. using curl (adapt the hostname, BucketFS port, bucket name and bucket write password).
```
curl -X PUT -T target/hadoop-etl-dist-1.0.0-SNAPSHOT.jar \
 http://w:write-password@your.exasol.host.com:2580/bucket1/hadoop-etl-dist-1.0.0-SNAPSHOT.jar
```

See chapter 3.6.4. "The synchronous cluster file system BucketFS" in the EXASolution User Manual for more details about BucketFS.


### 4. Deploy UDF Scripts

Then run the following SQL commands to deploy the UDF scripts in the database:
```
CREATE SCHEMA ETL;

--/
CREATE OR REPLACE JAVA SET SCRIPT IMPORT_HCAT_TABLE(...) EMITS (...) AS
%scriptclass com.exasol.hadoop.scriptclasses.ImportHCatTable;
%jar /buckets/your-bucket-fs/your-bucket/hadoop-etl-dist-1.0.0-SNAPSHOT.jar;
/

--/
CREATE OR REPLACE JAVA SET SCRIPT IMPORT_HIVE_TABLE_FILES(...) EMITS (...) AS
%env LD_LIBRARY_PATH=/tmp/;
%scriptclass com.exasol.hadoop.scriptclasses.ImportHiveTableFiles;
%jar /buckets/your-bucket-fs/your-bucket/hadoop-etl-dist-1.0.0-SNAPSHOT.jar;
/

--/
CREATE OR REPLACE JAVA SCALAR SCRIPT HCAT_TABLE_FILES(...)
 EMITS (
  hdfs_server_port VARCHAR(200),
  hdfspath VARCHAR(200),
  hdfs_user_or_service_principal VARCHAR(100),
  hcat_user_or_service_principal VARCHAR(100),
  input_format VARCHAR(200),
  serde VARCHAR(200),
  column_info VARCHAR(100000),
  partition_info VARCHAR(10000),
  serde_props VARCHAR(10000),
  import_partition INT,
  auth_type VARCHAR(1000),
  conn_name VARCHAR(1000),
  output_columns VARCHAR(100000),
  enable_rpc_encryption VARCHAR(100),
  debug_address VARCHAR(200))
 AS
%scriptclass com.exasol.hadoop.scriptclasses.HCatTableFiles;
%jar /buckets/your-bucket-fs/your-bucket/hadoop-etl-dist-1.0.0-SNAPSHOT.jar;
/

--/
CREATE OR REPLACE JAVA SET SCRIPT EXPORT_HCAT_TABLE(...) EMITS (...) AS
%scriptclass com.exasol.hadoop.scriptclasses.ExportHCatTable;
%jar /buckets/your-bucket-fs/your-bucket/hadoop-etl-dist-1.0.0-SNAPSHOT.jar;
/

--/
CREATE OR REPLACE JAVA SET SCRIPT EXPORT_INTO_HIVE_TABLE(...) EMITS (ROWS_AFFECTED INT) AS
%scriptclass com.exasol.hadoop.scriptclasses.ExportIntoHiveTable;
%jar /buckets/your-bucket-fs/your-bucket/hadoop-etl-dist-1.0.0-SNAPSHOT.jar;
/
```

### 5. Kerberos Authentication

If your Hadoop installation is secured by Kerberos, please see [Kerberos Authentication](kerberos.md) for setup details.
