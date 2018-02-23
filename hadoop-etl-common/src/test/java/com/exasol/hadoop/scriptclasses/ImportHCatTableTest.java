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

import com.exasol.ExaImportSpecification;
import com.exasol.ExaMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ImportHCatTableTest {
    
    public void addMandatoryParamsBasicAuth(Map<String, String> params) {
        params.put("HCAT_DB", "hcat_db");
        params.put("HCAT_TABLE", "hcat_table");
        params.put("HCAT_ADDRESS", "hcat_address");
        params.put("HDFS_USER", "hdfs_user");
        params.put("HCAT_USER", "hcat_user");
    }

    public void addMandatoryParamsKerberos(Map<String, String> params) {
        params.put("HCAT_DB", "hcat_db");
        params.put("HCAT_TABLE", "hcat_table");
        params.put("HCAT_ADDRESS", "hcat_address");
        params.put("AUTH_TYPE", "kerberos");
        params.put("KERBEROS_CONNECTION", "MyKerberosConn");
        params.put("KERBEROS_HDFS_SERVICE_PRINCIPAL", "hdfs_service_principal");
        params.put("KERBEROS_HCAT_SERVICE_PRINCIPAL", "hcat_service_principal");
    }
    
    @Test
    public void testGenerateSqlForImportSpecMandatory() {
        
        ExaImportSpecification importSpec = mock(ExaImportSpecification.class);
        ExaMetadata meta = mock(ExaMetadata.class);
        
        Map<String, String> params = new HashMap<>();
        addMandatoryParamsBasicAuth(params);
        when(importSpec.getParameters()).thenReturn(params);
        
        when(importSpec.hasConnectionInformation()).thenReturn(false);
        when(importSpec.hasConnectionName()).thenReturn(false);
        when(importSpec.isSubselect()).thenReturn(false);
        
        String sql = ImportHCatTable.generateSqlForImportSpec(meta, importSpec);
        sql = normalizeSql(sql);
        
        String sqlExpected = "SELECT"
                + " " + meta.getScriptSchema() +".IMPORT_HIVE_TABLE_FILES(hdfspath, input_format, serde, column_info, partition_info, serde_props, hdfs_server_port, hdfs_user, auth_type, conn_name, output_columns, enable_rpc_encryption, debug_address)"
                + " FROM ("
                + " SELECT " + meta.getScriptSchema() +".HCAT_TABLE_FILES('hcat_db', 'hcat_table', 'hcat_address', 'hdfs_user', 'hcat_user', nproc(), '', '', '', '', '', 'false', '')"
                + ") GROUP BY import_partition;";
        sqlExpected = normalizeSql(sqlExpected);
        
        assertEquals(sqlExpected, sql);
        
        verify(importSpec, atLeastOnce()).isSubselect();
        verify(importSpec, atLeastOnce()).getParameters();
        verify(importSpec, never()).getSubselectColumnNames();
        verify(importSpec, never()).getSubselectColumnSqlTypes();
        verify(importSpec, atLeastOnce()).hasConnectionInformation();
        verify(importSpec, atLeastOnce()).hasConnectionName();
        verify(importSpec, never()).getConnectionInformation();
        verify(importSpec, never()).getConnectionName();
    }
    
    @Test
    public void testGenerateSqlForImportSpecSubselect() {
        
        ExaImportSpecification importSpec = mock(ExaImportSpecification.class);
        
        ExaMetadata meta = mock(ExaMetadata.class);
        
        Map<String, String> params = new HashMap<>();
        addMandatoryParamsKerberos(params);
        params.put("PARALLELISM", "nproc()");
        params.put("PARTITIONS", "p1=01");
        params.put("OUTPUT_COLUMNS", "f1[0],f2");
        params.put("HDFS_URL", "hdfs://custom");
        params.put("DEBUG_ADDRESS", "host:1234");
        when(importSpec.getParameters()).thenReturn(params);
        
        List<String> subSelColNames = new ArrayList<>();
        subSelColNames.add("c1");
        subSelColNames.add("c2");
        List<String> subColTypes = new ArrayList<>();
        subColTypes.add("DECIMAL(16,0)");
        subColTypes.add("VARCHAR(1000)");
        when(importSpec.getSubselectColumnNames()).thenReturn(subSelColNames);
        when(importSpec.getSubselectColumnSqlTypes()).thenReturn(subColTypes);
        when(importSpec.isSubselect()).thenReturn(true);
        
        when(importSpec.hasConnectionInformation()).thenReturn(false);
        when(importSpec.hasConnectionName()).thenReturn(false);
        
        String sql = ImportHCatTable.generateSqlForImportSpec(meta, importSpec);
        sql = normalizeSql(sql);
        
        String sqlExpected = "SELECT"
                + " " + meta.getScriptSchema() +".IMPORT_HIVE_TABLE_FILES(hdfspath, input_format, serde, column_info, partition_info, serde_props, hdfs_server_port, hdfs_user, auth_type, conn_name, output_columns, enable_rpc_encryption, debug_address)"
                + " EMITS (\"c1\" DECIMAL(16,0),\"c2\" VARCHAR(1000)) FROM ("
                + " SELECT " + meta.getScriptSchema() +".HCAT_TABLE_FILES('hcat_db', 'hcat_table', 'hcat_address', 'hdfs_service_principal', 'hcat_service_principal', nproc(), 'p1=01', 'f1[0],f2', 'hdfs://custom', 'kerberos', 'MyKerberosConn', 'false', 'host:1234')"
                + ") GROUP BY import_partition;";
        sqlExpected = normalizeSql(sqlExpected);
        
        assertEquals(sqlExpected, sql);
        
        verify(importSpec, atLeastOnce()).isSubselect();
        verify(importSpec, atLeastOnce()).getParameters();
        verify(importSpec, atLeastOnce()).getSubselectColumnNames();
        verify(importSpec, atLeastOnce()).getSubselectColumnSqlTypes();
        verify(importSpec, atLeastOnce()).hasConnectionInformation();
        verify(importSpec, atLeastOnce()).hasConnectionName();
        verify(importSpec, never()).getConnectionInformation();
        verify(importSpec, never()).getConnectionName();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testGenerateSqlForImportSpecInvalidSubselect() {
        
        ExaImportSpecification importSpec = mock(ExaImportSpecification.class);
        
        ExaMetadata meta = mock(ExaMetadata.class);
        
        Map<String, String> params = new HashMap<>();
        addMandatoryParamsBasicAuth(params);
        when(importSpec.getParameters()).thenReturn(params);
        
        List<String> subSelColNames = new ArrayList<>();
        List<String> subColTypes = new ArrayList<>();
        when(importSpec.getSubselectColumnNames()).thenReturn(subSelColNames);
        when(importSpec.getSubselectColumnSqlTypes()).thenReturn(subColTypes);
        when(importSpec.isSubselect()).thenReturn(true);
        
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("In case of IMPORT in a subselect you need to specify the output columns, e.g. 'INSERT INTO (a int, b varchar(100)) ...'.");
        ImportHCatTable.generateSqlForImportSpec(meta, importSpec);
    }
    
    @Test
    public void testGenerateSqlForImportSpecMissingProperty() {
        
        ExaImportSpecification importSpec = mock(ExaImportSpecification.class);
        
        ExaMetadata meta = mock(ExaMetadata.class);
        
        Map<String, String> params = new HashMap<>();
        addMandatoryParamsBasicAuth(params);
        params.remove("HCAT_DB");
        when(importSpec.getParameters()).thenReturn(params);
        
        List<String> subSelColNames = new ArrayList<>();
        List<String> subColTypes = new ArrayList<>();
        when(importSpec.getSubselectColumnNames()).thenReturn(subSelColNames);
        when(importSpec.getSubselectColumnSqlTypes()).thenReturn(subColTypes);
        when(importSpec.isSubselect()).thenReturn(true);
        
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("The mandatory property HCAT_DB was not defined. Please specify it and run the statement again.");
        ImportHCatTable.generateSqlForImportSpec(meta, importSpec);
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
