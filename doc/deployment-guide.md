## Deploying the Hadoop ETL UDFs Step By Step

Run the following steps to deploy your UDF scripts:

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
  * Kerberos: If your Hadoop uses Kerberos authentication, the UDFs will authenticate using a keytab file. Each EXASOL node needs access to the Kerberos KDC (key distribution center), running on port ```88```. The KDC is configured in the kerberos config file which is used for the authentication, as described in the [Kerberos Authentication](#kerberos-authentication) section.

### 2. Building from Source

First clone the repository on your computer.
```
git clone https://github.com/EXASOL/hadoop-etl-udfs.git
cd hadoop-etl-udfs
```

You have to build the sources depending on your Hive and Hadoop version as follows. The resulting fat JAR (including all dependencies) is stored in ```target/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar```.

#### Cloudera CDH
You can look up the version numbers for Hadoop and Hive in the [CDH Maven documentation](https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_vd_cdh5_maven_repo.html) (search for the artifactId ```hadoop-common``` and ```hive-serde```).
```
mvn clean -DskipTests package assembly:single -P cloudera -Dhadoop.version=2.5.0-cdh5.2.0 -Dhive.version=0.13.1-cdh5.2.0
```

#### Hortonworks HDP
You can look up the version numbers in the HDP release notes.
```
mvn clean -DskipTests package assembly:single -P hortonworks -Dhadoop.version=2.7.1.2.3.0.0-2557 -Dhive.version=1.2.1.2.3.0.0-2557
```

#### Other Hadoop Distributions
You may have to add a Maven repository to pom.xml for your distribution. Then you can compile similarly to examples above for other distributions.

#### Standard Apache Hadoop and Hive (no distribution)
```
mvn clean -DskipTests package assembly:single -P-cloudera -Dhadoop.version=1.2.1 -Dhive.version=1.2.1
```
This command deactivates the Cloudera Maven profile which is active by default.

### 3. Upload Jar

You have to upload the jar to a bucket of your choice in the EXASOL bucket file system (BucketFS). This will allow using the jar in the UDF scripts.

Following steps are required to upload a file to a bucket:
* Make sure you have a bucket file system (BucketFS) and you know the port for either http or https. This can be done in EXAOperation under "EXABuckets". E.g. the id could be ```bucketfs1``` and the http port 2580.
* Check if you have a bucket in the BucketFS. Simply click on the name of the BucketFS in EXAOperation and add a bucket there, e.g. ```bucket1```. Also make sure you know the write password. For simplicity we assume that the bucket is defined as a public bucket, i.e. it can be read by any script.
* Now upload the file into this bucket, e.g. using curl (adapt the hostname, BucketFS port, bucket name and bucket write password).
```
curl -X PUT -T target/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar \
 http://w:write-password@your.exasol.host.com:2580/bucket1/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar
```

See chapter 3.6.4. "The synchronous cluster file system BucketFS" in the EXASolution User Manual for more details about BucketFS.


### 4. Deploy UDF Scripts

Then run the following SQL commands to deploy the UDF scripts in the database:
```
CREATE SCHEMA ETL;

CREATE OR REPLACE JAVA SET SCRIPT IMPORT_HCAT_TABLE(...) EMITS (...) AS
%scriptclass com.exasol.hadoop.scriptclasses.ImportHCatTable;
%jar /buckets/your-bucket-fs/your-bucket/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar;
/

CREATE OR REPLACE JAVA SET SCRIPT IMPORT_HIVE_TABLE_FILES(...) EMITS (...) AS
%env LD_LIBRARY_PATH=/tmp/;
%scriptclass com.exasol.hadoop.scriptclasses.ImportHiveTableFiles;
%jar /buckets/your-bucket-fs/your-bucket/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar;
/

CREATE OR REPLACE JAVA SCALAR SCRIPT HCAT_TABLE_FILES(...)
 EMITS (
  hdfs_server_port VARCHAR(200),
  hdfspath VARCHAR(200),
  hdfs_user VARCHAR(100),
  input_format VARCHAR(200),
  serde VARCHAR(200),
  column_info VARCHAR(100000),
  partition_info VARCHAR(10000),
  serde_props VARCHAR(10000),
  import_partition INT,
  auth_type VARCHAR(1000),
  conn_name VARCHAR(1000),
  output_columns VARCHAR(100000),
  debug_address VARCHAR(200))
 AS
%scriptclass com.exasol.hadoop.scriptclasses.HCatTableFiles;
%jar /buckets/your-bucket-fs/your-bucket/exa-hadoop-etl-udfs-0.0.1-SNAPSHOT-all-dependencies.jar;
/
```

### 5. Kerberos Authentication

This section only applies if your Hadoop is secured by Kerberos.

First, obtain the following information:
* Kerberos principal for Hadoop (i.e., Hadoop user)
* Kerberos configuration file (e.g., krb5.conf)
* Kerberos keytab which contains keys for the Kerberos principal
* Kerberos principal for the Hadoop NameNode (value of ```dfs.namenode.kerberos.principal``` in hdfs-site.xml)

In order for the UDFs to have access to the necessary Kerberos information, a CONNECTION object must be created in EXASOL. Storing the Kerberos information in CONNECTION objects provides the ability to set the accessibility of the Kerberos authentication data (especially the keytab) for users. The ```TO``` field is left empty, the Kerberos principal is stored in the ```USER``` field, and the Kerberos configuration and keytab are stored in the ```IDENTIFIED BY``` field (base64 format) along with an internal key to identify the CONNECTION as a Kerberos CONNECTION.

In order to simplify the creation of Kerberos CONNECTION objects, the [create_kerberos_conn.py](tools/create_kerberos_conn.py) Python script has been provided. The script requires 4 arguments:
* CONNECTION name
* Kerberos principal
* Kerberos configuration file path
* Kerberos keytab path

Example command:
```
python tools/create_kerberos_conn.py krb_conn krbuser@EXAMPLE.COM /etc/krb5.conf ./krbuser.keytab
```
Output:
```sql
CREATE CONNECTION krb_conn TO '' USER 'krbuser@EXAMPLE.COM' IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```
The output is a CREATE CONNECTION statement, which can be executed directly in EXASOL to create the Kerberos CONNECTION object. For more detailed information about the script, use the help option:
```
python tools/create_kerberos_conn.py -h
```

You can then grant access to the CONNECTION to UDFs and users:
```sql
GRANT ACCESS ON CONNECTION krb_conn FOR ETL.HCAT_TABLE_FILES TO exauser;
GRANT ACCESS ON CONNECTION krb_conn FOR ETL.IMPORT_HIVE_TABLE_FILES TO exauser;
```
Or, if you want to grant the user access to the CONNECTION in any UDF (which means that the user can access all the information in the CONNECTION--most importantly the keytab):
```sql
GRANT CONNECTION krb_conn TO exauser;
```

Then, you can access the created CONNECTION from a UDF by passing the CONNECTION name as a UDF parameter as described above. Note: The ```hcat-and-hdfs-user``` UDF parameter must be set the NameNode principal, as described above.

Example:
```sql
IMPORT INTO (code VARCHAR(1000), description VARCHAR (1000), total_emp INT, salary INT)
FROM SCRIPT ETL.IMPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'sample_07'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs/_HOST@EXAMPLE.COM'
 AUTH_TYPE       = 'kerberos'
 AUTH_KERBEROS_CONNECTION = 'krb_conn';
```
