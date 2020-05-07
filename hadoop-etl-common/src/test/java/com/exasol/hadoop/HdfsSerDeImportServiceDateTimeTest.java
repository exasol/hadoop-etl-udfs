package com.exasol.hadoop;

import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.exasol.ExaIterator;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.jsonpath.OutputColumnSpec;
import com.exasol.jsonpath.OutputColumnSpecUtil;
import com.exasol.utils.UdfUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HdfsSerDeImportServiceDateTimeTest {

    private Configuration conf;
    private File temporaryFile;
    private FileSystem fileSystem;

    @Before
    public void before() throws Exception {
        conf = new Configuration();
        fileSystem = FileSystem.getLocal(conf);
        temporaryFile = File.createTempFile("rcfile_", "test");
    }

     @After
     public void after() throws Exception {
         temporaryFile.delete();
     }

    // Intentional copy paste from HdfsSerDeImportServiceTest for now.
    private void runImportRCFile(ExaIterator ctx, List<HCatTableColumn> columns,
            List<HCatTableColumn> partitionColumns, List<OutputColumnSpec> outputColumns,
            String file) throws Exception {
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>(
                Arrays.asList(new HCatSerDeParameter("serialization.format", "1")));

        String inputFormatClassName = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
        String serDeClassName = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";

        List<String> hdfsServers = new ArrayList<>(Arrays.asList("file:///"));

        InputFormat<?, ?> inputFormat =
            (InputFormat<?, ?>) UdfUtils.getInstanceByName(inputFormatClassName);
        AbstractSerDe serDe = (AbstractSerDe) UdfUtils.getInstanceByName(serDeClassName);
        HdfsSerDeImportService.importFile(fileSystem, file, partitionColumns, inputFormat, serDe,
                serDeParameters, hdfsServers, "hdfs", columns, outputColumns, false, false, ctx);
    }

    @Test
    public void testImportDateTypeAsSQLDate() throws Exception {
        final RCFileWriter.Type type = RCFileWriter.Type.DATE;
        final RCFileWriter rcWriter = new RCFileWriter(conf, fileSystem, type);

        final List<Date> values = new ArrayList<>();
        int count = 0;
        for (int i = 31230; i <= 31234; i++) {
            count++;
            values.add(Date.ofEpochDay(i));
        }
        rcWriter.writeValues(temporaryFile, values);

        ExaIterator ctx = mock(ExaIterator.class);
        final List<HCatTableColumn> columns = Arrays.asList(
                new HCatTableColumn(type.getColumnName(), type.getColumnType()));
        final List<OutputColumnSpec> outputColumns = OutputColumnSpecUtil
            .generateDefaultOutputSpecification(columns, Collections.emptyList());

        runImportRCFile(ctx, columns, Collections.emptyList(), outputColumns,
                temporaryFile.toURI().toString());

        verify(ctx, times(count)).emit(anyVararg());
        for (final Date date : values) {
            verify(ctx, times(1)).emit(eq(new java.sql.Date(date.toEpochMilli())));
        }
    }

    @Test
    public void testImportTimestampTypeAsSQLTimestamp() throws Exception {
        final RCFileWriter.Type type = RCFileWriter.Type.TIMESTAMP;
        final RCFileWriter rcWriter = new RCFileWriter(conf, fileSystem, type);

        final List<Timestamp> values = new ArrayList<>();
        int count = 0;
        for (int i = 31230; i <= 31234; i++) {
            count++;
            values.add(Timestamp.ofEpochMilli(i));
        }
        rcWriter.writeValues(temporaryFile, values);

        ExaIterator ctx = mock(ExaIterator.class);
        final List<HCatTableColumn> columns = Arrays.asList(
                new HCatTableColumn(type.getColumnName(), type.getColumnType()));
        final List<OutputColumnSpec> outputColumns = OutputColumnSpecUtil
            .generateDefaultOutputSpecification(columns, Collections.emptyList());

        runImportRCFile(ctx, columns, Collections.emptyList(), outputColumns,
                temporaryFile.toURI().toString());

        verify(ctx, times(count)).emit(anyVararg());
        for (final Timestamp ts : values) {
            verify(ctx, times(1)).emit(eq(new java.sql.Timestamp(ts.toEpochMilli())));
        }
    }

}
