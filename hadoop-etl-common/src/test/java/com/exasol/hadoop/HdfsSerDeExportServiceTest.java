package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.ExaIteratorDummy;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hdfs.HdfsService;
import com.exasol.hadoop.parquet.ExaParquetTypeInfo;
import com.exasol.jsonpath.OutputColumnSpec;
import com.exasol.jsonpath.OutputColumnSpecUtil;
import com.exasol.utils.UdfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.math.BigDecimal;
import java.sql.Timestamp;
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

    static final int FIRST_DATA_COLUMN = 11; // UDF argument number that has the first data column (see ExportIntoHiveTable class)

    static final String PARQUET_INPUT_FORMAT_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    static final String PARQUET_SERDE_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

/*
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
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "tinyintnull", OriginalType.INT_8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "smallintnull", OriginalType.INT_16));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "intnull", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "bigintnull", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "floatnull", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "doublenull", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 4,"decimalnull"));



        List<String> colNames = new ArrayList<>();
        List<TypeInfo> colTypes = new ArrayList<>();
        colNames.add("ti");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("tinyint"));
        colNames.add("si");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("smallint"));
        colNames.add("i");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("int"));
        colNames.add("bi");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
        colNames.add("f");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("float"));
        colNames.add("d");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("double"));
        colNames.add("dec1");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("decimal(36,0)"));
        colNames.add("dec2");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("decimal(36,5)"));
        colNames.add("dec3");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("decimal(8,8)"));
        colNames.add("tinyintnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("tinyint"));
        colNames.add("smallintnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("smallint"));
        colNames.add("intnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("int"));
        colNames.add("bigintnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
        colNames.add("floatnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("float"));
        colNames.add("doublenull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("double"));
        colNames.add("decimalnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("decimal(8,8)"));







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
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter, colNames, colTypes);

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
        columns.add(new HCatTableColumn("tinyintnull", "tinyint"));
        columns.add(new HCatTableColumn("smallintnull", "smallint"));
        columns.add(new HCatTableColumn("intnull", "int"));
        columns.add(new HCatTableColumn("bigintnull", "bigint"));
        columns.add(new HCatTableColumn("floatnull", "float"));
        columns.add(new HCatTableColumn("doublenull", "double"));
        columns.add(new HCatTableColumn("decimalnull", "decimal(8,8)"));

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
                eq(new BigDecimal("0.12345678")),
                eq(null),
                eq(null),
                eq(null),
                eq(null),
                eq(null),
                eq(null),
                eq(null)
        );
    }
*/

    @Test
    public void testExportParquetTimestamp() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();

        /*
        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "t1", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "t2", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "timestampnull", null));
        */
        List<ExaParquetTypeInfo> schemaTypes = new ArrayList<>();
        schemaTypes.add(new ExaParquetTypeInfo("t1","OPTIONAL", "INT96"));
        schemaTypes.add(new ExaParquetTypeInfo("t2","OPTIONAL", "INT96"));
        schemaTypes.add(new ExaParquetTypeInfo("timestampnull","OPTIONAL", "INT96"));





        List<String> colNames = new ArrayList<>();
        List<TypeInfo> colTypes = new ArrayList<>();
        colNames.add("t1");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("timestamp"));
        colNames.add("t2");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("timestamp"));
        colNames.add("timestampnull");
        colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo("timestamp"));




        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        // Hive automatically adjusts values to UTC when reading (Impala does not)
        ZonedDateTime zdtUtc1 = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime zdtUtc2 = zdtUtc1.minusMonths(6).minusHours(12);
        ZonedDateTime zdtDefault1 = zdtUtc1.withZoneSameInstant(ZoneId.of(TimeZone.getDefault().getID().toString()));
        ZonedDateTime zdtDefault2 = zdtUtc2.withZoneSameInstant(ZoneId.of(TimeZone.getDefault().getID().toString()));
        row.add(Timestamp.valueOf(zdtUtc1.toLocalDateTime()));
        row.add(Timestamp.valueOf(zdtUtc2.toLocalDateTime()));
        row.add(null);
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter, colNames, colTypes);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("t1", "timestamp"));
        columns.add(new HCatTableColumn("t2", "timestamp"));
        columns.add(new HCatTableColumn("timestampnull", "timestamp"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(
                eq(Timestamp.valueOf(zdtDefault1.toLocalDateTime())),
                eq(Timestamp.valueOf(zdtDefault2.toLocalDateTime())),
                eq(null)
        );
    }

/*
    @Test
    public void testExportParquetBoolean() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<Type> schemaTypes = new ArrayList<>();
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "b1", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "b2", null));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "booleannull", null));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add(Boolean.TRUE);
        row.add(Boolean.FALSE);
        row.add(null);
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter, null, null);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("b1", "boolean"));
        columns.add(new HCatTableColumn("b2", "boolean"));
        columns.add(new HCatTableColumn("booleannull", "boolean"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(
                eq(Boolean.TRUE),
                eq(Boolean.FALSE),
                eq(null)
        );
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
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "charnull", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "varcharnull", OriginalType.UTF8));
        schemaTypes.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "stringnull", OriginalType.UTF8));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add("a");
        row.add("aaaaaaaaaa");
        row.add("b");
        row.add("bbbbbbbbbb");
        row.add("cccccccccccccccccccc");
        row.add(null);
        row.add(null);
        row.add(null);
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter, null, null);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("c1", "char(1)"));
        columns.add(new HCatTableColumn("c2", "char(12)"));
        columns.add(new HCatTableColumn("v1", "varchar(1)"));
        columns.add(new HCatTableColumn("v2", "varchar(10)"));
        columns.add(new HCatTableColumn("s1", "string"));
        columns.add(new HCatTableColumn("charnull", "char(1)"));
        columns.add(new HCatTableColumn("varcharnulll", "varchar(1)"));
        columns.add(new HCatTableColumn("stringnull", "string"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(
                eq("a"),
                eq("aaaaaaaaaa  "),
                eq("b"),
                eq("bbbbbbbbbb"),
                eq("cccccccccccccccccccc"),
                eq(null),
                eq(null),
                eq(null)
        );
    }
    */

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

}
