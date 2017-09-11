package com.exasol.hadoop.scriptclasses;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exasol.ExaExportSpecification;
import com.exasol.ExaMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExportHCatTableTest {

    public void addMandatoryParams(Map<String, String> params) {
        params.put("HCAT_DB", "hcat_db");
        params.put("HCAT_TABLE", "hcat_table");
        params.put("HCAT_ADDRESS", "");
        params.put("HDFS_USER", "hdfs_user");
        params.put("UNIT_TEST_MODE", "true"); // Only used for unit testing
    }

    @Test
    public void testGenerateSqlForExportSpecMandatory() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);
        when(exportSpec.hasCreatedBy()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
        sql = normalizeSql(sql);

        String sqlExpected = "SELECT"
                + " \"" + meta.getScriptSchema() +"\".\"EXPORT_INTO_HIVE_TABLE\"("
                + "'hcat_db', 'hcat_table', '', 'hdfs_user', '', '', '', '', '', 'uncompressed', ''"
                + ", \"COL1\""
                + ") "
                + "FROM "
                + "DUAL;";
        sqlExpected = normalizeSql(sqlExpected);

        assertEquals(sqlExpected, sql);

        verify(exportSpec, never()).hasConnectionName();
        verify(exportSpec, never()).getConnectionName();
        verify(exportSpec, never()).hasConnectionInformation();
        verify(exportSpec, never()).getConnectionInformation();
        verify(exportSpec, atLeastOnce()).getParameters();
        verify(exportSpec, atLeastOnce()).hasTruncate();
        verify(exportSpec, atLeastOnce()).hasReplace();
        verify(exportSpec, atLeastOnce()).hasCreatedBy();
        verify(exportSpec, never()).getCreatedBy();
        verify(exportSpec, atLeastOnce()).getSourceColumnNames();
    }

    @Test
    public void testGenerateSqlForExportStaticPartition() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        params.put("STATIC_PARTITION", "part1=2015-01-01/part2=EU");
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);
        when(exportSpec.hasCreatedBy()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
        sql = normalizeSql(sql);

        String sqlExpected = "SELECT"
                + " \"" + meta.getScriptSchema() +"\".\"EXPORT_INTO_HIVE_TABLE\"("
                + "'hcat_db', 'hcat_table', '', 'hdfs_user', '', 'part1=2015-01-01/part2=EU', '', '', '', 'uncompressed', ''"
                + ", \"COL1\""
                + ") "
                + "FROM "
                + "DUAL;";
        sqlExpected = normalizeSql(sqlExpected);

        assertEquals(sqlExpected, sql);

        verify(exportSpec, never()).hasConnectionName();
        verify(exportSpec, never()).getConnectionName();
        verify(exportSpec, never()).hasConnectionInformation();
        verify(exportSpec, never()).getConnectionInformation();
        verify(exportSpec, atLeastOnce()).getParameters();
        verify(exportSpec, atLeastOnce()).hasTruncate();
        verify(exportSpec, atLeastOnce()).hasReplace();
        verify(exportSpec, atLeastOnce()).hasCreatedBy();
        verify(exportSpec, never()).getCreatedBy();
        verify(exportSpec, atLeastOnce()).getSourceColumnNames();
    }

    @Test
    public void testGenerateSqlForExportDynamicPartition() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        params.put("DYNAMIC_PARTITION_EXA_COLS", "\"col2\"/COL3");
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);
        when(exportSpec.hasCreatedBy()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        colNames.add("col2");
        colNames.add("COL3");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
        sql = normalizeSql(sql);

        String sqlExpected = "SELECT"
                + " \"" + meta.getScriptSchema() +"\".\"EXPORT_INTO_HIVE_TABLE\"("
                + "'hcat_db', 'hcat_table', '', 'hdfs_user', '', '', '1,2', '', '', 'uncompressed', ''"
                + ", \"COL1\", \"col2\", \"COL3\""
                + ") "
                + "FROM "
                + "DUAL"
                + " GROUP BY \"col2\", \"COL3\";";
        sqlExpected = normalizeSql(sqlExpected);

        assertEquals(sqlExpected, sql);

        verify(exportSpec, never()).hasConnectionName();
        verify(exportSpec, never()).getConnectionName();
        verify(exportSpec, never()).hasConnectionInformation();
        verify(exportSpec, never()).getConnectionInformation();
        verify(exportSpec, atLeastOnce()).getParameters();
        verify(exportSpec, atLeastOnce()).hasTruncate();
        verify(exportSpec, atLeastOnce()).hasReplace();
        verify(exportSpec, atLeastOnce()).hasCreatedBy();
        verify(exportSpec, never()).getCreatedBy();
        verify(exportSpec, atLeastOnce()).getSourceColumnNames();
    }

    @Test
    public void testGenerateSqlForExportCompression() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        params.put("COMPRESSION_TYPE", "snappy");
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);
        when(exportSpec.hasCreatedBy()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
        sql = normalizeSql(sql);

        String sqlExpected = "SELECT"
                + " \"" + meta.getScriptSchema() +"\".\"EXPORT_INTO_HIVE_TABLE\"("
                + "'hcat_db', 'hcat_table', '', 'hdfs_user', '', '', '', '', '', 'snappy', ''"
                + ", \"COL1\""
                + ") "
                + "FROM "
                + "DUAL;";
        sqlExpected = normalizeSql(sqlExpected);

        assertEquals(sqlExpected, sql);

        verify(exportSpec, never()).hasConnectionName();
        verify(exportSpec, never()).getConnectionName();
        verify(exportSpec, never()).hasConnectionInformation();
        verify(exportSpec, never()).getConnectionInformation();
        verify(exportSpec, atLeastOnce()).getParameters();
        verify(exportSpec, atLeastOnce()).hasTruncate();
        verify(exportSpec, atLeastOnce()).hasReplace();
        verify(exportSpec, atLeastOnce()).hasCreatedBy();
        verify(exportSpec, never()).getCreatedBy();
        verify(exportSpec, atLeastOnce()).getSourceColumnNames();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGenerateSqlForExportTruncateWithoutJdbcConnection() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(true);
        when(exportSpec.hasReplace()).thenReturn(false);
        when(exportSpec.hasCreatedBy()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("The JDBC_CONNECTION parameter is required, but was not specified.");
        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
    }

    @Test
    public void testGenerateSqlForExportCreatedByWithoutJdbcConnection() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);
        when(exportSpec.hasCreatedBy()).thenReturn(true);
        when(exportSpec.getCreatedBy()).thenReturn("create table test(c1 int)");

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("The JDBC_CONNECTION parameter is required, but was not specified.");
        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
    }

    @Test
    public void testGenerateSqlForExportReplaceWithoutJdbcConnection() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(true);
        when(exportSpec.hasCreatedBy()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("The JDBC_CONNECTION parameter is required, but was not specified.");
        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
    }

    @Test
    public void testGenerateSqlForExportMissingProperty() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        params.remove("HCAT_TABLE");
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("The mandatory property HCAT_TABLE was not defined. Please specify it and run the statement again.");
        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
    }

    @Test
    public void testGenerateSqlForExportStaticDynamicPartitions() {

        ExaExportSpecification exportSpec = mock(ExaExportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        when(meta.getScriptSchema()).thenReturn("hcat_schema");

        Map<String, String> params = new HashMap<>();
        addMandatoryParams(params);
        params.put("STATIC_PARTITION", "part1=2015-01-01/part2=EU");
        params.put("DYNAMIC_PARTITION_EXA_COLS", "\"col2\"/COL3");
        when(exportSpec.getParameters()).thenReturn(params);

        when(exportSpec.hasConnectionInformation()).thenReturn(false);
        when(exportSpec.hasConnectionName()).thenReturn(false);
        when(exportSpec.hasTruncate()).thenReturn(false);
        when(exportSpec.hasReplace()).thenReturn(false);

        List<String> colNames = new ArrayList<>();
        colNames.add("COL1");
        when(exportSpec.getSourceColumnNames()).thenReturn(colNames);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Static and dynamic partitions cannot both be specified.");
        String sql = ExportHCatTable.generateSqlForExportSpec(meta, exportSpec);
    }

    /**
     * Convert newlines, tabs, and double whitespaces to whitespaces. At the end only single whitespaces remain.
     */
    private static String normalizeSql(String sql) {
        return sql.replaceAll("\t", " ")
                .replaceAll("\n", " ")
                .replaceAll("\\s+", " ");
    }
}
