package com.exasol.adapter;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class HiveAdapterTest {

    @Test
    @Ignore
    public void testReadMetadata() throws SQLException {

        String hiveDB = "xperience";

        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HiveAdapterProperties.PROP_SCHEMA_NAME, hiveDB);
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("DUMMY_SCHEMA", "", properties);

        ExaConnectionInformation connectionInfo = new ExaConnectionInformation() {
            @Override
            public ConnectionType getType() {
                return ConnectionType.PASSWORD;
            }

            @Override
            public String getAddress() {
                return "thrift://cloudera01.exacloud.de:9083";
            }

            @Override
            public String getUser() {
                return "hdfs";
            }

            @Override
            public String getPassword() {
                return "hdfs";
            }
        };

        SchemaMetadata schemaMetadata = HiveAdapter.readMetadata(schemaMetadataInfo, connectionInfo,null,null);

        long numTables = 11;
        assertEquals(numTables, schemaMetadata.getTables().size());

        TableMetadata t1Meta = schemaMetadata.getTables().get(0);
        assertEquals("all_hive_data_types", t1Meta.getName());

        //original type array<string>
        assertEquals("VARCHAR(2000000) UTF8", t1Meta.getColumns().get(0).getType().toString());
        //original type bigint
        assertEquals("DECIMAL(36, 0)", t1Meta.getColumns().get(1).getType().toString());
        //original type boolean
        assertEquals("BOOLEAN", t1Meta.getColumns().get(2).getType().toString());
        //original type char(1)
        assertEquals("CHAR(1) UTF8", t1Meta.getColumns().get(3).getType().toString());
        //original type decimal(10,0)
        assertEquals("DECIMAL(10, 0)", t1Meta.getColumns().get(4).getType().toString());
        //original type double
        assertEquals("DOUBLE", t1Meta.getColumns().get(5).getType().toString());
        //original type float
        assertEquals("DOUBLE", t1Meta.getColumns().get(6).getType().toString());
        //original type int
        assertEquals("DECIMAL(36, 0)", t1Meta.getColumns().get(7).getType().toString());
        //original type map<string,int>
        assertEquals("VARCHAR(2000000) UTF8", t1Meta.getColumns().get(8).getType().toString());
        //original type smallint
        assertEquals("DECIMAL(36, 0)", t1Meta.getColumns().get(9).getType().toString());
        //original type string
        assertEquals("VARCHAR(2000000) UTF8", t1Meta.getColumns().get(10).getType().toString());
        //original type struct<a:string,b:struct<c:int>>
        assertEquals("VARCHAR(2000000) UTF8", t1Meta.getColumns().get(11).getType().toString());
        //original type timestamp
        assertEquals("TIMESTAMP", t1Meta.getColumns().get(12).getType().toString());
        //original type tinyint
        assertEquals("DECIMAL(36, 0)", t1Meta.getColumns().get(13).getType().toString());
        //original type varchar(10)
        assertEquals("VARCHAR(10) UTF8", t1Meta.getColumns().get(14).getType().toString());
        //original type binary
        assertEquals("VARCHAR(2000000) UTF8", t1Meta.getColumns().get(15).getType().toString());
        //original type date
        assertEquals("DATE", t1Meta.getColumns().get(16).getType().toString());


        // TODO compare the whole metadata vs expected
    }
}
