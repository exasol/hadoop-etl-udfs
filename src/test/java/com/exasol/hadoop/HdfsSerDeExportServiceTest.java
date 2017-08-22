package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.ExaIteratorDummy;
import com.exasol.ExaMetadata;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.hdfs.HdfsService;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.exasol.hadoop.scriptclasses.ExportIntoHiveTable;
import com.exasol.jsonpath.OutputColumnSpec;
import com.exasol.jsonpath.OutputColumnSpecUtil;
import com.exasol.utils.UdfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.io.File;

import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HdfsSerDeExportServiceTest {

    /**
     * Execute
     * ssh ws64-2.dev.exasol.com -L 9083:vm031.cos.dev.exasol.com:9083 -L 8020:vm031.cos.dev.exasol.com:8020 -L 8888:vm031.cos.dev.exasol.com:8888
     * before running this test
     */

    static final int FIRST_DATA_COLUMN = 11; // UDF argument number that has the first data column (see ExportIntoHiveTable class)

    static final String PARQUET_INPUT_FORMAT_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    static final String PARQUET_SERDE_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testExportParquetNumeric() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "ti", OriginalType.INT_8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "si", OriginalType.INT_16));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "i", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "bi", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "f", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "d", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 15,"dec1"));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 15,"dec2"));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 4,"dec3"));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add(55);
        row.add(5555);
        row.add(555555555);
        row.add(555555555555555555L);
        row.add(55.55);
        row.add(55555.55555);
        row.add(new BigDecimal("555555555555555555555555555555555555"));
        row.add(new BigDecimal("5555555555555555555555555555555.55555"));
        row.add(new BigDecimal("0.12345678"));
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("ti", "tinyint"));
        columns.add(new HCatTableColumn("si", "smallint"));
        columns.add(new HCatTableColumn("i", "int"));
        columns.add(new HCatTableColumn("bi", "bigint"));
        columns.add(new HCatTableColumn("f", "float"));
        columns.add(new HCatTableColumn("d", "double"));
        columns.add(new HCatTableColumn("dec1", "decimal(36,0)"));
        columns.add(new HCatTableColumn("dec2", "decimal(36,5)"));
        columns.add(new HCatTableColumn("dec3", "decimal(8,8)"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(
                eq((byte)55),
                eq((short)5555),
                eq(555555555),
                eq(555555555555555555L),
                eq(55.55f),
                eq(55555.55555),
                eq(new BigDecimal("555555555555555555555555555555555555")),
                eq(new BigDecimal("5555555555555555555555555555555.55555")),
                eq(new BigDecimal("0.12345678")));
    }

    @Test
    public void testExportParquetTimestamp() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "t1", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "t2", null));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        // Hive automatically adjusts values to UTC when reading (Impala does not)
        ZonedDateTime zdtUtc1 = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime zdtUtc2 = zdtUtc1.minusMonths(6).minusHours(12);
        ZonedDateTime zdtDefault1 = zdtUtc1.withZoneSameInstant(ZoneId.of(TimeZone.getDefault().getID().toString()));
        ZonedDateTime zdtDefault2 = zdtUtc2.withZoneSameInstant(ZoneId.of(TimeZone.getDefault().getID().toString()));
        row.add(Timestamp.valueOf(zdtUtc1.toLocalDateTime()));
        row.add(Timestamp.valueOf(zdtUtc2.toLocalDateTime()));
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("t1", "timestamp"));
        columns.add(new HCatTableColumn("t2", "timestamp"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq(Timestamp.valueOf(zdtDefault1.toLocalDateTime())), eq(Timestamp.valueOf(zdtDefault2.toLocalDateTime())));
    }

    @Test
    public void testExportParquetBoolean() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "b1", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "b2", null));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add(Boolean.TRUE);
        row.add(Boolean.FALSE);
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("b1", "boolean"));
        columns.add(new HCatTableColumn("b2", "boolean"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq(Boolean.TRUE), eq(Boolean.FALSE));
    }

    @Test
    public void testExportParquetString() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c2", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "v1", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "v2", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "s1", OriginalType.UTF8));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add("a");
        row.add("aaaaaaaaaa");
        row.add("b");
        row.add("bbbbbbbbbb");
        row.add("cccccccccccccccccccc");
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("c1", "char(1)"));
        columns.add(new HCatTableColumn("c2", "char(12)"));
        columns.add(new HCatTableColumn("v1", "varchar(1)"));
        columns.add(new HCatTableColumn("v2", "varchar(10)"));
        columns.add(new HCatTableColumn("s1", "string"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq("a"), eq("aaaaaaaaaa  "), eq("b"), eq("bbbbbbbbbb"), eq("cccccccccccccccccccc"));
    }

    private void addRow(List<List<Object>> dataSet, List<Object> row) {
        // Insert null values for non-data columns of data set
        for (int i = 0; i < FIRST_DATA_COLUMN; i++) {
            row.add(0, null);
        }
        dataSet.add(row);
    }

    private void importFile(ExaIterator ctx, List<HCatTableColumn> columns, List<HCatTableColumn> partitionColumns, String file, String inputFormatName, String serdeName) throws Exception {
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        serDeParameters.add(new HCatSerDeParameter("serialization.format", "1"));
        String hdfsUser = "hdfs";
        boolean useKerberos = false;
        List<String> hdfsServers = new ArrayList<>();
        hdfsServers.add("file:///");
        final Configuration conf = new Configuration();
        FileSystem fs = HdfsService.getFileSystem(hdfsServers, conf);
        InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) UdfUtils.getInstanceByName(inputFormatName);
        SerDe serDe = (SerDe) UdfUtils.getInstanceByName(serdeName);
        List<OutputColumnSpec> outputColumns = OutputColumnSpecUtil.generateDefaultOutputSpecification(columns, new ArrayList<HCatTableColumn>());
        HdfsSerDeImportService.importFile(fs, file, partitionColumns, inputFormat, serDe, serDeParameters, hdfsServers, hdfsUser, columns, outputColumns, useKerberos, ctx);
    }




    //@Test
    public void exportToTableUdf() throws Exception {
        String dbName = "default";
        String table = "parquet_partition";
        String hiveMetastoreURL = "thrift://cloudera01.exacloud.de:9083";
        String hdfsUser = "hdfs";
        String hdfsUrl = "";
        String partition = "part1=a/part2=1";
        String authType = "";
        String authKerberosConnection = "";
        String debugAddress = "";

        List<List<Object>> iterValues = new ArrayList<>();
        List<Object> iterValue = new ArrayList<>();
        iterValue.add(dbName);
        iterValue.add(table);
        iterValue.add(hiveMetastoreURL);
        iterValue.add(hdfsUser);
        iterValue.add(hdfsUrl);
        iterValue.add(partition);
        iterValue.add(authType);
        iterValue.add(authKerberosConnection);
        iterValue.add(debugAddress);
        iterValues.add(iterValue);

        ExaMetadata meta = null;
        ExaIterator iter = new ExaIteratorDummy(iterValues);
        ExportIntoHiveTable.run(meta, iter);
    }

    //@Test
    public void exportToTable() throws Exception {
        String dbName = "default";
        //String table = "sample_07_parquet";
        String table = "parquet_partition";
        String hiveMetastoreURL = "thrift://cloudera01.exacloud.de:9083";
        String hdfsURL = "hdfs://cloudera01.exacloud.de:8020/user/hive/warehouse/" + table;
        String partition = "part1=a/part2=1";
        if (partition != null && !partition.isEmpty())
            hdfsURL += "/" + partition;
        HCatTableMetadata tableMeta = null;
        tableMeta = HiveMetastoreService.getTableMetadata(hiveMetastoreURL, dbName, table, false, "");
        System.out.println("tableMeta: " + tableMeta);

        List<List<Object>> rows = new ArrayList<>();
        List<Class<?>> rowTypes = new ArrayList<>();
        List<Object> rowValues = new ArrayList<>();

        /*
        // PARQUET_NUM_TYPES
        rowTypes.add(Class.forName("java.lang.Integer"));       rowValues.add(55);
        rowTypes.add(Class.forName("java.lang.Integer"));       rowValues.add(5555);
        rowTypes.add(Class.forName("java.lang.Integer"));       rowValues.add(555555555);
        rowTypes.add(Class.forName("java.lang.Long"));          rowValues.add(555555555555555555L);
        //rowTypes.add(Class.forName("java.lang.Float"));       //rowValues.add(55.55f);
        rowTypes.add(Class.forName("java.lang.Double"));        rowValues.add(55.55);
        rowTypes.add(Class.forName("java.lang.Double"));        rowValues.add(55555.55555);
        rowTypes.add(Class.forName("java.math.BigDecimal"));    rowValues.add(new BigDecimal("55555555555555555555555555555555555555"));
        rowTypes.add(Class.forName("java.math.BigDecimal"));    rowValues.add(new BigDecimal("555555555555555555555555555555555.55555"));
        rowTypes.add(Class.forName("java.math.BigDecimal"));    rowValues.add(new BigDecimal("0.12345678"));

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "ti", OriginalType.INT_8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "si", OriginalType.INT_16));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "i", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "bi", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "f", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "d", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16,"dec1"));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16,"dec2"));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 4,"dec3"));
        */

        /*
        // PARQUET STRING TYPES
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("str_str_str_str");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("c");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("c_c_c_c");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("v");
        rowTypes.add(Class.forName("java.lang.String"));       rowValues.add("v_v_v_v");

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "str", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c2", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "vc2", OriginalType.UTF8));
        */

        /*
        // PARQUET DATE TIME TYPES
        rowTypes.add(Class.forName("java.sql.Timestamp"));  rowValues.add(Timestamp.valueOf("1945-05-24 02:01:01.123456789"));

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "ts", null));
        */

        /*
        // PARQUET BOOLEAN BINARY TYPES
        rowTypes.add(Class.forName("java.lang.Boolean"));   rowValues.add(false);
        rowTypes.add(Class.forName("java.lang.String"));    rowValues.add("fdsa");

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "bool", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "bin", null));
        */

        ///*
        // PARQUET SNAPPY TYPES
        rowTypes.add(Class.forName("java.lang.Integer"));   rowValues.add(1111);
        rowTypes.add(Class.forName("java.lang.String"));    rowValues.add("aaaa");

        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "c1", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "c2", null));
        //*/

        rows.add(rowValues);

        //String testPartition = "part1=d/part2=4";
        //boolean createdPartition = HiveMetastoreService.createPartitionIfNotExists(hiveMetastoreURL,false, "", dbName, table, testPartition);
        //System.out.println("Created partition " + testPartition + ": " + createdPartition);

        String hdfsUser = "hdfs";
        StringBuilder sb = new StringBuilder();
        sb.append("exa_export_");
        sb.append(new SimpleDateFormat("yyyyMMdd_HHmmss_").format(new Date()));
        sb.append(UUID.randomUUID().toString().replaceAll("-", ""));
        sb.append(".parq");
        String filename = sb.toString();
        System.out.println("Temp Filename: " + filename);

        //HdfsSerDeExportService.exportToParquetTableTest(hdfsURL, hdfsUser, filename, tableMeta, schemaTypes, new ExaIteratorDummy(rows));
    }

}
