# Google DataProc Integration Issues

It is possible to integrate Google DataProc cluster and Hadoop ETL UDFs.
However, one single update should be applied to Hive Metastore inside the
DataProc cluster.

## Problem

The returned hdfs location is encoded as `hdfs://cluster-name`. This does not
work because somehow, even is both dataproc cluster and exasol clusters are
inside same subnet, exasol nodes cannot find the dns `cluster-name`.

## Solution

Change the metastore fs location to internal ip address.

```bash
# From inside the DataProc cluster

# Get the metastore fs location
hive --service metatool -listFSRoot

# Update the fs root to ip
hive --metatool -updateLocation hdfs://192.168.0.5 hdfs://cluster-name
```

The ip address provided for the new location is the ip address of metastore
(usually the name node) in the DataProc cluster.
