# Hadoop ETL UDFs

[![Build Status](https://travis-ci.org/EXASOL/hadoop-etl-udfs.svg?branch=master)](https://travis-ci.org/EXASOL/hadoop-etl-udfs)


###### Please note that this is an open source project which is officially supported by EXASOL. For any question, you can contact our support team.

## Table of Contents
1. [Overview](#overview)
2. [Getting Started](#getting-started)
3. [Using the Hadoop ETL UDFs](#using-the-hadoop-etl-udfs)
    1. [IMPORT](#import)
    2. [EXPORT](#export)
4. [Debugging](#debugging)


## Overview
Hadoop ETL UDFs are the main way to load data from Hadoop into EXASOL (HCatalog tables on HDFS).


## Getting Started

Before you can start using the Hadoop ETL UDFs you have to deploy the UDFs in your EXASOL database.
Please follow the [step-by-step deployment guide](doc/deployment-guide.md).



## Using the Hadoop ETL UDFs

The following examples assume that you have simple authentication. If your Hadoop requires Kerberos authentication, please refer to the [Kerberos Authentication](doc/deployment-guide.md#5-kerberos-authentication) section.

### IMPORT

Please see the [IMPORT details](doc/import.md).

### EXPORT

Please see the [EXPORT details](doc/export.md).


## Debugging
To see debug output relating to Hadoop and the UDFs, you can use the Python script [udf_debug.py](tools/udf_debug.py).

First, start the udf_debug.py script, which will listen at the specified address and port and print all incoming text.
```
python tools/udf_debug.py -s myhost -p 3000
```
Then set the ```DEBUG_ADDRESS``` UDF arguments so that stdout of the UDFs will be forwarded to the specified address.
```sql
IMPORT FROM SCRIPT ETL.IMPORT_HCAT_TABLE WITH
 HCAT_DB         = 'default'
 ...
 DEBUG_ADDRESS   = 'myhost:3000';
```
