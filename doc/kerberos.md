## Kerberos Authentication

These instructions only apply if your Hadoop installation is secured by Kerberos.

### Prerequisites

First, obtain the following information:
* Kerberos principal for Hadoop (i.e., Hadoop user)
* Kerberos configuration file (e.g., krb5.conf)
* Kerberos keytab which contains keys for the Kerberos principal
* Kerberos principal for the Hadoop NameNode (value of ```dfs.namenode.kerberos.principal``` in hdfs-site.xml)

If you plan to use an optional JDBC connection for EXPORT:
* Kerberos principal for Hive (value of ```hive.server2.authentication.kerberos.principal``` in hive-site.xml)

### Create a Connection

In order for the UDFs to have access to the necessary Kerberos information, a CONNECTION object must be created in EXASOL. Storing the Kerberos information in CONNECTION objects provides the ability to set the accessibility of the Kerberos authentication data (especially the keytab) for users. The ```TO``` field is left empty, the Kerberos principal is stored in the ```USER``` field, and the Kerberos configuration and keytab are stored in the ```IDENTIFIED BY``` field (base64 format) along with an internal key to identify the CONNECTION as a Kerberos CONNECTION.

In order to simplify the creation of Kerberos CONNECTION objects, the [create_kerberos_conn.py](../tools/create_kerberos_conn.py) Python script has been provided. The script requires 4 arguments:
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

### JDBC Kerberos Connection for EXPORT

In order to use the ```REPLACE```, ```TRUNCATE```, or ```CREATED BY``` options with the EXPORT UDF, you must also setup a CONNECTION for the JDBC driver. This CONNECTION has the same format as the above-created CONNECTION, except that the JDBC connection string must be provided in the ```TO``` field.

Assuming the principal information for the JDBC connection is the same as for connection created above, you can just copy the CREATE CONNECTION statement previously created, specify a new name, and provide the JDBC connection string in TO field. Otherwise, you will need to execute the [create_kerberos_conn.py](../tools/create_kerberos_conn.py) Python script again with the correct information for the JDBC user.

The EXPORT UDF uses the Apache Hive JDBC driver to execute the necessary statements. For most cases, the connection string should have a format similar to the following. See [JDBC Client Setup for a Secure Cluster](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBCClientSetupforaSecureCluster) for details.
```
'jdbc:hive2://hive-host:10000/;principal=hive/hive-host@EXAMPLE'
```

Then you can create the JDBC CONNECTION using the statement below.
```sql
CREATE CONNECTION jdbc_krb_conn TO 'jdbc:hive2://hive-host:10000/;principal=hive/hive-host@EXAMPLE' USER 'krbuser@EXAMPLE.COM' IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='
```

### Grant Access to the Connection

You can then grant access to the CONNECTION to UDFs and users:
```sql
GRANT ACCESS ON CONNECTION krb_conn FOR ETL.HCAT_TABLE_FILES TO exauser;
GRANT ACCESS ON CONNECTION krb_conn FOR ETL.IMPORT_HIVE_TABLE_FILES TO exauser;
GRANT ACCESS ON CONNECTION krb_conn FOR ETL.EXPORT_HCAT_TABLE TO exauser;
GRANT ACCESS ON CONNECTION krb_conn FOR ETL.EXPORT_INTO_HIVE_TABLE TO exauser;
```
For optional an optional JDBC connection for EXPORT:
```sql
GRANT ACCESS ON CONNECTION jdbc_krb_conn FOR ETL.EXPORT_HCAT_TABLE TO exauser;
```

Or, if you want to grant the user access to the CONNECTION in any UDF (which means that the user can access all the information in the CONNECTION--most importantly the keytab):
```sql
GRANT CONNECTION krb_conn TO exauser;
```

### Connection Usage

Then, you can access the created CONNECTION from a UDF by passing the CONNECTION name as a UDF parameter as described above. Note: The ```hcat-and-hdfs-user``` UDF parameter must be set the NameNode principal, as described above.

IMPORT Example:
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

EXPORT Example:
```sql
EXPORT
TABLE1
INTO SCRIPT ETL.EXPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 HCAT_TABLE      = 'test_table'
 HCAT_ADDRESS    = 'thrift://hive-metastore-host:9083'
 HDFS_USER       = 'hdfs/_HOST@EXAMPLE.COM';
 AUTH_TYPE       = 'kerberos'
 AUTH_KERBEROS_CONNECTION = 'krb_conn';
 JDBC_AUTH_TYPE  = 'kerberos'
 JDBC_CONNECTION = 'jdbc_krb_conn'
CREATED BY 'CREATE TABLE default.test_table(col1 VARCHAR(200)) STORED AS PARQUET';
```
