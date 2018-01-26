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

    @Test
    public void testExportParquetNumeric() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<ExaParquetTypeInfo> schemaTypes = new ArrayList<>();
        schemaTypes.add(new ExaParquetTypeInfo("ti","OPTIONAL", "INT32", "INT_8"));
        schemaTypes.add(new ExaParquetTypeInfo("si","OPTIONAL", "INT32", "INT_16"));
        schemaTypes.add(new ExaParquetTypeInfo("i","OPTIONAL", "INT32"));
        schemaTypes.add(new ExaParquetTypeInfo("bi","OPTIONAL", "INT64"));
        schemaTypes.add(new ExaParquetTypeInfo("f","OPTIONAL", "FLOAT"));
        schemaTypes.add(new ExaParquetTypeInfo("d","OPTIONAL", "DOUBLE"));
        schemaTypes.add(new ExaParquetTypeInfo("dec1","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 15));
        schemaTypes.add(new ExaParquetTypeInfo("dec2","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 15));
        schemaTypes.add(new ExaParquetTypeInfo("dec3","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("tinyintnull","OPTIONAL", "INT32", "INT_8"));
        schemaTypes.add(new ExaParquetTypeInfo("smallintnull","OPTIONAL", "INT32", "INT_16"));
        schemaTypes.add(new ExaParquetTypeInfo("intnull","OPTIONAL", "INT32"));
        schemaTypes.add(new ExaParquetTypeInfo("bigintnull","OPTIONAL", "INT64"));
        schemaTypes.add(new ExaParquetTypeInfo("floatnull","OPTIONAL", "FLOAT"));
        schemaTypes.add(new ExaParquetTypeInfo("doublenull","OPTIONAL", "DOUBLE"));
        schemaTypes.add(new ExaParquetTypeInfo("decimalnull","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));

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

    @Test
    public void testExportParquetTimestamp() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<ExaParquetTypeInfo> schemaTypes = new ArrayList<>();
        schemaTypes.add(new ExaParquetTypeInfo("t1","OPTIONAL", "INT96"));
        schemaTypes.add(new ExaParquetTypeInfo("t2","OPTIONAL", "INT96"));
        schemaTypes.add(new ExaParquetTypeInfo("timestampnull","OPTIONAL", "INT96"));

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

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

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

    @Test
    public void testExportParquetBoolean() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<ExaParquetTypeInfo> schemaTypes = new ArrayList<>();
        schemaTypes.add(new ExaParquetTypeInfo("b1","OPTIONAL", "BOOLEAN"));
        schemaTypes.add(new ExaParquetTypeInfo("b2","OPTIONAL", "BOOLEAN"));
        schemaTypes.add(new ExaParquetTypeInfo("booleannull","OPTIONAL", "BOOLEAN"));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add(Boolean.TRUE);
        row.add(Boolean.FALSE);
        row.add(null);
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

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
        List<ExaParquetTypeInfo> schemaTypes = new ArrayList<>();
        schemaTypes.add(new ExaParquetTypeInfo("c1","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("c2","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("v1","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("v2","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("s1","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("charnull","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("varcharnull","OPTIONAL", "BINARY", "UTF8"));
        schemaTypes.add(new ExaParquetTypeInfo("stringnull","OPTIONAL", "BINARY", "UTF8"));

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

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

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

    @Test
    public void testExportDecimalPadding() throws Exception {

        List<Integer> dynamicCols = new ArrayList<>();
        List<ExaParquetTypeInfo> schemaTypes = new ArrayList<>();
        schemaTypes.add(new ExaParquetTypeInfo("dec1","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 2));
        schemaTypes.add(new ExaParquetTypeInfo("dec1neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 2));
        schemaTypes.add(new ExaParquetTypeInfo("dec2","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 3));
        schemaTypes.add(new ExaParquetTypeInfo("dec2neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 3));
        schemaTypes.add(new ExaParquetTypeInfo("dec3","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("dec3neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("dec4","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 6));
        schemaTypes.add(new ExaParquetTypeInfo("dec4neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 6));
        schemaTypes.add(new ExaParquetTypeInfo("dec5","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("dec5neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("dec6","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 7));
        schemaTypes.add(new ExaParquetTypeInfo("dec6neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 7));
        schemaTypes.add(new ExaParquetTypeInfo("dec7","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 8));
        schemaTypes.add(new ExaParquetTypeInfo("dec7neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 9));
        schemaTypes.add(new ExaParquetTypeInfo("dec8","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 6));
        schemaTypes.add(new ExaParquetTypeInfo("dec8neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 6));
        schemaTypes.add(new ExaParquetTypeInfo("dec9","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 12));
        schemaTypes.add(new ExaParquetTypeInfo("dec9neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 12));
        schemaTypes.add(new ExaParquetTypeInfo("dec10","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 14));
        schemaTypes.add(new ExaParquetTypeInfo("dec10neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 14));
        schemaTypes.add(new ExaParquetTypeInfo("dec11","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 16));
        schemaTypes.add(new ExaParquetTypeInfo("dec11neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 16));
        schemaTypes.add(new ExaParquetTypeInfo("dec12","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("dec12neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 4));
        schemaTypes.add(new ExaParquetTypeInfo("dec13","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 5));
        schemaTypes.add(new ExaParquetTypeInfo("dec13neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 5));
        schemaTypes.add(new ExaParquetTypeInfo("dec14","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 10));
        schemaTypes.add(new ExaParquetTypeInfo("dec14neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 10));
        schemaTypes.add(new ExaParquetTypeInfo("dec15","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 16));
        schemaTypes.add(new ExaParquetTypeInfo("dec15neg","OPTIONAL", "FIXED_LEN_BYTE_ARRAY", 16));

        List<List<Object>> dataSet = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add(new BigDecimal("1")); row.add(new BigDecimal("-1"));
        row.add(new BigDecimal("255")); row.add(new BigDecimal("-255"));
        row.add(new BigDecimal("256")); row.add(new BigDecimal("-256"));
        row.add(new BigDecimal("1023")); row.add(new BigDecimal("-1023"));
        row.add(new BigDecimal("64000")); row.add(new BigDecimal("-64000"));
        row.add(new BigDecimal("72000")); row.add(new BigDecimal("-72000"));
        row.add(new BigDecimal("16777211")); row.add(new BigDecimal("-16777211"));
        row.add(new BigDecimal("4294968234")); row.add(new BigDecimal("-4294968234"));
        row.add(new BigDecimal("87509812398888234")); row.add(new BigDecimal("-87509812398888234"));
        row.add(new BigDecimal("698234872384787230490982344234")); row.add(new BigDecimal("-698234872384787230490982344234"));
        row.add(new BigDecimal("871234870814250986098124309821039456")); row.add(new BigDecimal("-871234870814250986098124309821039456"));
        row.add(new BigDecimal("1.123")); row.add(new BigDecimal("-1.123"));
        row.add(new BigDecimal("6400.2")); row.add(new BigDecimal("-6400.2"));
        row.add(new BigDecimal("8750981239.8888234")); row.add(new BigDecimal("-8750981239.8888234"));
        row.add(new BigDecimal("0.871234870814250986098124309821039456")); row.add(new BigDecimal("-0.871234870814250986098124309821039456"));
        addRow(dataSet, row);
        ExaIterator iter = new ExaIteratorDummy(dataSet);

        File tempFile = new File(testFolder.getRoot(),UUID.randomUUID().toString().replaceAll("-", "") + ".parq");

        HdfsSerDeExportService.exportToParquetTable(testFolder.getRoot().toString(), "hdfs", false, null, tempFile.getName(), null, "uncompressed", schemaTypes, FIRST_DATA_COLUMN, dynamicCols, iter);

        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("dec1", "decimal(1,0)"));
        columns.add(new HCatTableColumn("dec1neg", "decimal(1,0)"));
        columns.add(new HCatTableColumn("dec2", "decimal(4,0)"));
        columns.add(new HCatTableColumn("dec2neg", "decimal(4,0)"));
        columns.add(new HCatTableColumn("dec3", "decimal(5,0)"));
        columns.add(new HCatTableColumn("dec3neg", "decimal(6,0)"));
        columns.add(new HCatTableColumn("dec4", "decimal(15,0)"));
        columns.add(new HCatTableColumn("dec4neg", "decimal(15,0)"));
        columns.add(new HCatTableColumn("dec5", "decimal(5,0)"));
        columns.add(new HCatTableColumn("dec5neg", "decimal(5,0)"));
        columns.add(new HCatTableColumn("dec6", "decimal(7,0)"));
        columns.add(new HCatTableColumn("dec6neg", "decimal(7,0)"));
        columns.add(new HCatTableColumn("dec7", "decimal(11,0)"));
        columns.add(new HCatTableColumn("dec7neg", "decimal(11,0)"));
        columns.add(new HCatTableColumn("dec8", "decimal(16,0)"));
        columns.add(new HCatTableColumn("dec8neg", "decimal(16,0)"));
        columns.add(new HCatTableColumn("dec9", "decimal(19,0)"));
        columns.add(new HCatTableColumn("dec9neg", "decimal(19,0)"));
        columns.add(new HCatTableColumn("dec10", "decimal(33,0)"));
        columns.add(new HCatTableColumn("dec10neg", "decimal(33,0)"));
        columns.add(new HCatTableColumn("dec11", "decimal(36,0)"));
        columns.add(new HCatTableColumn("dec11neg", "decimal(36,0)"));
        columns.add(new HCatTableColumn("dec12", "decimal(5,3)"));
        columns.add(new HCatTableColumn("dec12neg", "decimal(5,3)"));
        columns.add(new HCatTableColumn("dec13", "decimal(5,1)"));
        columns.add(new HCatTableColumn("dec13neg", "decimal(5,1)"));
        columns.add(new HCatTableColumn("dec14", "decimal(17,7)"));
        columns.add(new HCatTableColumn("dec14neg", "decimal(17,7)"));
        columns.add(new HCatTableColumn("dec15", "decimal(38,36)"));
        columns.add(new HCatTableColumn("dec15neg", "decimal(38,36)"));

        List<HCatTableColumn> partitionColumns = null;
        importFile(ctx, columns, partitionColumns, tempFile.getCanonicalPath(), PARQUET_INPUT_FORMAT_CLASS_NAME, PARQUET_SERDE_CLASS_NAME);

        int expectedNumRows = 1;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(
                eq(new BigDecimal("1")), eq(new BigDecimal("-1")),
                eq(new BigDecimal("255")), eq(new BigDecimal("-255")),
                eq(new BigDecimal("256")), eq(new BigDecimal("-256")),
                eq(new BigDecimal("1023")), eq(new BigDecimal("-1023")),
                eq(new BigDecimal("64000")), eq(new BigDecimal("-64000")),
                eq(new BigDecimal("72000")), eq(new BigDecimal("-72000")),
                eq(new BigDecimal("16777211")), eq(new BigDecimal("-16777211")),
                eq(new BigDecimal("4294968234")), eq(new BigDecimal("-4294968234")),
                eq(new BigDecimal("87509812398888234")), eq(new BigDecimal("-87509812398888234")),
                eq(new BigDecimal("698234872384787230490982344234")), eq(new BigDecimal("-698234872384787230490982344234")),
                eq(new BigDecimal("871234870814250986098124309821039456")), eq(new BigDecimal("-871234870814250986098124309821039456")),
                eq(new BigDecimal("1.123")), eq(new BigDecimal("-1.123")),
                eq(new BigDecimal("6400.2")), eq(new BigDecimal("-6400.2")),
                eq(new BigDecimal("8750981239.8888234")), eq(new BigDecimal("-8750981239.8888234")),
                eq(new BigDecimal("0.871234870814250986098124309821039456")), eq(new BigDecimal("-0.871234870814250986098124309821039456"))
        );
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

}
