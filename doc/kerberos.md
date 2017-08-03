
This section only applies if your Hadoop is secured by Kerberos.

First, obtain the following information:
* Kerberos principal for Hadoop (i.e., Hadoop user)
* Kerberos configuration file (e.g., krb5.conf)
* Kerberos keytab which contains keys for the Kerberos principal
* Kerberos principal for the Hadoop NameNode (value of ```dfs.namenode.kerberos.principal``` in hdfs-site.xml)

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
