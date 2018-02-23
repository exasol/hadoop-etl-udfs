package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.hadoop.hcat.HCatSerDeParameter;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hdfs.HdfsService;
import com.exasol.jsonpath.OutputColumnSpec;
import com.exasol.jsonpath.OutputColumnSpecUtil;
import com.exasol.utils.UdfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class HdfsSerDeImportServiceTest {

    @Test
    public void testImportSample07Rc() throws Exception {
        // test resources from src/test/resources are copied to target/test-classes, and this folder is the classpath of the junit test. However, classpath doesn't help here, so we need to hardcode the path.
        String file = "target/test-classes/sample_07_rc_cdh_5_4_8";
        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = getSample07Columns();
        runImportRCFile(ctx, columns, new ArrayList<HCatTableColumn>(), OutputColumnSpecUtil.generateDefaultOutputSpecification(columns, new ArrayList<HCatTableColumn>()), file);

        int expectedNumRows = 823;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
    }
    
    @Test
    public void testImportSample07RcSmall() throws Exception {
        String file = "target/test-classes/sample_07_rc_5rows_cdh_5_4_8";
        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = getSample07Columns();
        runImportRCFile(ctx, columns, new ArrayList<HCatTableColumn>(), OutputColumnSpecUtil.generateDefaultOutputSpecification(columns, new ArrayList<HCatTableColumn>()), file);

        int expectedNumRows = 5;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq("00-0000"), eq("All Occupations"), eq(134354250), eq(40690));
        verify(ctx, times(1)).emit(eq("11-0000"), eq("Management occupations"), eq(6003930), eq(96150));
        verify(ctx, times(1)).emit(eq("11-1011"), eq("Chief executives"), eq(299160), eq(151370));
        verify(ctx, times(1)).emit(eq("11-1021"), eq("General and operations managers"), eq(1655410), eq(103780));
        verify(ctx, times(1)).emit(eq("11-1031"), eq("Legislators"), eq(61110), eq(33880));
    }
    
    @Test
    public void testImportSample07RcSmallMixCols() throws Exception {
        String file = "target/test-classes/sample_07_rc_5rows_cdh_5_4_8";
        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = getSample07Columns();
        List<OutputColumnSpec> outputColumns = OutputColumnSpecUtil.parseOutputSpecification("salary, total_emp, description, code, salary", columns);
        runImportRCFile(ctx, columns, new ArrayList<HCatTableColumn>(), outputColumns, file);

        int expectedNumRows = 5;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq(40690), eq(134354250), eq("All Occupations"), eq("00-0000"), eq(40690));
        verify(ctx, times(1)).emit(eq(96150), eq(6003930), eq("Management occupations"), eq("11-0000"), eq(96150));
        verify(ctx, times(1)).emit(eq(151370), eq(299160), eq("Chief executives"), eq("11-1011"), eq(151370));
        verify(ctx, times(1)).emit(eq(103780), eq(1655410), eq("General and operations managers"), eq("11-1021"), eq(103780));
        verify(ctx, times(1)).emit(eq(33880), eq(61110), eq("Legislators"), eq("11-1031"), eq(33880));
    }

    private void runImportRCFile(ExaIterator ctx, List<HCatTableColumn> columns, List<HCatTableColumn> partitionColumns, List<OutputColumnSpec> outputColumns, String file) throws Exception {
        List<HCatSerDeParameter> serDeParameters = new ArrayList<>();
        serDeParameters.add(new HCatSerDeParameter("serialization.format", "1"));
        
        String inputFormatClassName = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
        String serDeClassName = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
        String hdfsUser = "hdfs";
        boolean useKerberos = false;
        
        List<String> hdfsServers = new ArrayList<>();
        hdfsServers.add("file:///");
        final Configuration conf = new Configuration();
        FileSystem fs = HdfsService.getFileSystem(hdfsServers,conf);
        
        InputFormat<?, ?> inputFormat = (InputFormat<?, ?>) UdfUtils.getInstanceByName(inputFormatClassName);
        SerDe serDe = (SerDe) UdfUtils.getInstanceByName(serDeClassName);
        HdfsSerDeImportService.importFile(fs, file, partitionColumns, inputFormat, serDe, serDeParameters, hdfsServers, hdfsUser, columns, outputColumns, useKerberos, false, ctx);
    }
    
    private List<HCatTableColumn> getSample07Columns() {
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("code", "string"));
        columns.add(new HCatTableColumn("description", "string"));
        columns.add(new HCatTableColumn("total_emp", "int"));
        columns.add(new HCatTableColumn("salary", "int"));
        return columns;
    }
    
    @Test
    public void testImportComplexRCFile() throws Exception {
        String file = "target/test-classes/complex_rc_cdh_5_4_8";
        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = getComplexColumns();
        runImportRCFile(ctx, columns, new ArrayList<HCatTableColumn>(), OutputColumnSpecUtil.generateDefaultOutputSpecification(columns, new ArrayList<HCatTableColumn>()), file);

        int expectedNumRows = 3;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq("[\"f1-a\",\"f1-b\"]"), eq("{\"k1\":\"a\",\"k2\":\"b\"}"), eq("{\"g1\":1,\"g2\":[1,2]}"), eq(1.2), eq("[[\"a\",\"b\"],[\"c\",\"d\"]]"), eq("{\"g1\":{\"h1\":{\"k1\":\"v1\",\"k2\":\"v2\"}}}"));
        verify(ctx, times(1)).emit(eq("[\"F1-a\",\"F1-b\"]"), eq("{\"K1\":\"a\",\"K2\":\"b\"}"), eq("{\"g1\":1,\"g2\":[1,2]}"), eq(1.2), eq("[[\"A\",\"B\"],[\"C\",\"D\"]]"), eq("{\"g1\":{\"h1\":{\"K1\":\"V1\",\"K2\":\"V2\"}}}"));
        // TODO Check what to do with \N values. They encoding a NULL value. See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Transform.
        verify(ctx, times(1)).emit(eq("[\"F1-a\"]"), eq("{\"K1\":\"a\"}"), eq("{\"g1\":1,\"g2\":[null]}"), eq(null), eq("[[\"A\",\"B\"],[\"C\",\"D\"]]"), eq("{\"g1\":{\"h1\":{\"\\\\N\":null}}}"));
    }
    
    @Test
    public void testImportComplexRCFileWithJsonPath() throws Exception {
        String file = "target/test-classes/complex_rc_cdh_5_4_8";
        ExaIterator ctx = mock(ExaIterator.class);
        List<HCatTableColumn> columns = getComplexColumns();
        List<OutputColumnSpec> outputColumns = OutputColumnSpecUtil.parseOutputSpecification("f1[0], f1[1], f2.K1, f3.g2, f4, f5, f5[1][0], f6.g1.h1.k1", columns);
        runImportRCFile(ctx, columns, new ArrayList<HCatTableColumn>(), outputColumns, file);

        int expectedNumRows = 3;
        verify(ctx, times(expectedNumRows)).emit(anyVararg());
        verify(ctx, times(1)).emit(eq("f1-a"), eq("f1-b"), eq(null), eq("[1,2]"), eq(1.2), eq("[[\"a\",\"b\"],[\"c\",\"d\"]]"), eq("c"), eq("v1"));
        verify(ctx, times(1)).emit(eq("F1-a"), eq("F1-b"), eq("a"), eq("[1,2]"), eq(1.2), eq("[[\"A\",\"B\"],[\"C\",\"D\"]]"), eq("C"), eq(null));
        // Structs with non-existing fields are mistakenly returned as arrays with a single null from SerDe. Seems to be a SerDe bug. Hive correctly shows NULL instead of [null] as we do
        verify(ctx, times(1)).emit(eq("F1-a"), eq(null), eq("a"), eq("[null]"), eq(null), eq("[[\"A\",\"B\"],[\"C\",\"D\"]]"), eq("C"), eq(null));
    }
    
    private List<HCatTableColumn> getComplexColumns() {
        List<HCatTableColumn> columns = new ArrayList<>();
        columns.add(new HCatTableColumn("f1", "array<string>"));
        columns.add(new HCatTableColumn("f2", "map<string,string>"));
        columns.add(new HCatTableColumn("f3", "struct<g1:int,g2:array<int>>"));
        columns.add(new HCatTableColumn("f4", "double"));
        columns.add(new HCatTableColumn("f5", "array<array<string>>"));
        columns.add(new HCatTableColumn("f6", "struct<g1:struct<h1:map<string,string>>>"));
        return columns;
    }
}
