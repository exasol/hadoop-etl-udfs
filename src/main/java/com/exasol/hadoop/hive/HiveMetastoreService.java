package com.exasol.hadoop.hive;

import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveMetastoreService {


    public static HiveMetaStoreClient getHiveMetastoreClient(String hiveMetastoreUrl,boolean useKerberos, String kerberosPrinciple,String password){
        try {
            return checkHiveMetaStoreClient(hiveMetastoreUrl,useKerberos,kerberosPrinciple,password);
        } catch (MetaException e) {
            throw new RuntimeException("Unknown MetaException occured when connecting to the Hive Metastore " + hiveMetastoreUrl + ": " + e.toString(), e);
        }
    }

    public static HiveMetaStoreClient checkHiveMetaStoreClient(String hiveMetastoreUrl,boolean useKerberos, String kerberosPrinciple,String password) throws MetaException {
        HiveConf hiveConf = new HiveConf(new Configuration(), HiveConf.class);
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreUrl);
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        if (useKerberos) {
            System.out.println("Add kerberosPrinciple: " + kerberosPrinciple);
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, kerberosPrinciple);
            if(password!=null) {
                String[] confKeytab = password.split(";");
                if (confKeytab.length != 3 || !confKeytab[0].equals("ExaAuthType=Kerberos")) {
                    throw new RuntimeException("An invalid Kerberos CONNECTION was specified.");
                }
                hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, confKeytab[2]);
            }
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
        }
        return new HiveMetaStoreClient(hiveConf);
    }

    /**
     * We use the HiveMetaStoreClient API to access the Hive Metastore Thrift Server.
     * Good example: https://github.com/apache/falcon/blob/master/common/src/main/java/org/apache/falcon/catalog/HiveCatalogService.java
     * There is an alternative higher level API, which does not deliver enough metadata: org.apache.hive.hcatalog.api.HCatClient (new version of org.apache.hcatalog.api.HCatClient)
     */
    public static HCatTableMetadata getTableMetadata(String hiveMetastoreUrl, String dbName, String tableName, boolean useKerberos, String kerberosPrinciple) {

        HiveMetaStoreClient client = getHiveMetastoreClient(hiveMetastoreUrl,useKerberos,kerberosPrinciple,null);
        Table table;
        try {
          table = client.getTable(dbName, tableName);
        } catch (MetaException e) {
            throw new RuntimeException("Unknown MetaException occured when reading table information for table " + tableName + " in database " + dbName + " from the Hive Metastore " + hiveMetastoreUrl + ": " + e.toString(), e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException("Table " + tableName + " in database " + dbName + " could not be found in Hive Metastore " + hiveMetastoreUrl + ". Error: " + e.toString(), e);
        } catch (TException e) {
            throw new RuntimeException("Unknown TException occured when reading table information for table " + tableName + " in database " + dbName + " from the Hive Metastore " + hiveMetastoreUrl + ": " + e.toString(), e);
        }
        String tableType = table.getTableType();
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        List<HCatTableColumn> partitionColumns = new ArrayList<>();
        for (FieldSchema partitionKey : partitionKeys) {
            partitionColumns.add(new HCatTableColumn(partitionKey.getName(), partitionKey.getType()));
        }
        // Get advanced information from StorageDescriptor
        StorageDescriptor sd = table.getSd();
        String location = sd.getLocation();
        String inputFormat = sd.getInputFormat();
        String serDeClass = sd.getSerdeInfo().getSerializationLib();
        List<FieldSchema> cols = sd.getCols();
        List<HCatTableColumn> columns = new ArrayList<>();
        for (FieldSchema col : cols) {
            columns.add(new HCatTableColumn(col.getName(), col.getType()));
        }
        Map<String, String> parameters = sd.getSerdeInfo().getParameters();
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        for (String key : parameters.keySet()) {
            serDeParameters.add(new HCatSerDeParameter(key, parameters.get(key)));
        }
        client.close();
        return new HCatTableMetadata(location, columns, partitionColumns, tableType, inputFormat, serDeClass, serDeParameters);
    }

}
