package com.exasol.hadoop.hcat;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WebHCatJsonParserTest {

    // This response contains only the relevant fields
    private static final String hcatResponse =
        "{" + 
        "    \"location\":\"hdfs://vm031.cos.dev.exasol.com:8020/user/hive/warehouse/albums_rc_multi_part\"," + 
        "    \"columns\":[" +
        "        {" +
        "            \"name\":\"artistid\"," +
        "            \"type\":\"int\"" +
        "        }," +
        "        {" +
        "            \"name\":\"name\"," +
        "            \"type\":\"string\"" +
        "        }," +
        "        {" +
        "            \"name\":\"year\"," +
        "            \"type\":\"int\"" +
        "        }" +
        "    ]," +
        "    \"partitionColumns\":[" +
        "        {" +
        "            \"name\":\"year_created\"," +
        "            \"type\":\"int\"" +
        "        }," +
        "        {" +
        "            \"name\":\"month_created\"," +
        "            \"type\":\"int\"" +
        "        }" +
        "    ]," +
        "    \"partitioned\":true," +
        "    \"inputFormat\":\"org.apache.hadoop.hive.ql.io.RCFileInputFormat\"," +
        "    \"sd\":{" +
        "        \"inputFormat\":\"org.apache.hadoop.hive.ql.io.RCFileInputFormat\"," +
        "        \"outputFormat\":\"org.apache.hadoop.hive.ql.io.RCFileOutputFormat\"," +
        "        \"serdeInfo\":{" +
        "            \"serializationLib\":\"org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe\"," +
        "            \"parameters\":{" +
        "                \"serialization.format\":\"1\"" +
        "            }" +
        "        }" +
        "    }," +
        "    \"tableType\":\"MANAGED_TABLE\"" +
        "}";
    
    @Test
    public void testParse() throws Exception
    {
        HCatTableMetadata meta = WebHCatJsonParser.parse(hcatResponse);
        
        assertEquals("hdfs://vm031.cos.dev.exasol.com:8020/user/hive/warehouse/albums_rc_multi_part", meta.getHdfsLocation());
        assertEquals("MANAGED_TABLE", meta.getTableType());
        assertEquals("org.apache.hadoop.hive.ql.io.RCFileInputFormat", meta.getInputFormatClass());
        assertEquals("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", meta.getSerDeClass());
        
        assertEquals(3, meta.getColumns().size());
        assertEquals("artistid", meta.getColumns().get(0).getName());
        assertEquals("int", meta.getColumns().get(0).getDataType());
        assertEquals("name", meta.getColumns().get(1).getName());
        assertEquals("string", meta.getColumns().get(1).getDataType());
        assertEquals("year", meta.getColumns().get(2).getName());
        assertEquals("int", meta.getColumns().get(2).getDataType());
        
        assertEquals(2, meta.getPartitionColumns().size());
        assertEquals("year_created", meta.getPartitionColumns().get(0).getName());
        assertEquals("int", meta.getPartitionColumns().get(0).getDataType());
        assertEquals("month_created", meta.getPartitionColumns().get(1).getName());
        assertEquals("int", meta.getPartitionColumns().get(1).getDataType());
        
        assertEquals(1, meta.getSerDeParameters().size());
        assertEquals("serialization.format", meta.getSerDeParameters().get(0).getName());
        assertEquals("1", meta.getSerDeParameters().get(0).getValue());
    }
}
