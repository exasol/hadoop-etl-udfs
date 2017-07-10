package com.exasol.adapter;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.request.*;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.exasol.utils.UdfUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveAdapter {

    public static String adapterCall(ExaMetadata meta, String input) throws Exception {
        String result = "";
        try {
            AdapterRequest request = new RequestJsonParser().parseRequest(input);

            switch (request.getType()) {
                case CREATE_VIRTUAL_SCHEMA:
                    result = handleCreateVirtualSchema((CreateVirtualSchemaRequest) request, meta);
                    break;
                case DROP_VIRTUAL_SCHEMA:
                    result = handleDropVirtualSchema();
                    break;

                case SET_PROPERTIES:
                    result = handleSetProperty((SetPropertiesRequest) request, meta);
                    break;

                case REFRESH:
                    result = handleRefresh((RefreshRequest) request, meta);
                    break;
                case GET_CAPABILITIES:
                    result = handleGetCapabilities();
                    break;

                case PUSHDOWN:
                    result = handlePushdownRequest((PushdownRequest) request, meta);
                    break;
                default:
                    throw new RuntimeException("Request Type not supported: " + request.getType());
            }

            return result;
        }
        catch (AdapterException ex){
            throw ex;
        }
        catch (Exception ex) {
            String stacktrace = UdfUtils.traceToString(ex);
            throw new Exception("Error in Adapter: " + ex.getMessage() + "\nStacktrace: " + stacktrace + "\nFor following request: " + input + "\nResponse: " + result);
        }
    }

    static SchemaMetadata readMetadata(SchemaMetadataInfo schemaMetaInfo, ExaConnectionInformation connection, List<String> tableNames, String newSchema) throws SQLException, MetadataException {
        String databaseName = newSchema;
        if (newSchema == null) {
            databaseName = HiveAdapterProperties.getSchema(schemaMetaInfo.getProperties());
        }


        HiveMetaStoreClient hiveMetastoreClient = HiveMetastoreService.getHiveMetastoreClient(connection.getAddress(), HiveQueryGenerator.isKerberosAuth(connection.getPassword()), connection.getUser().replaceAll("hdfs", "hive"));
        if (tableNames == null) {
            try {
                tableNames = hiveMetastoreClient.getAllTables(databaseName);
            } catch (MetaException e) {
                e.getMessage();
            }
        }
        List<TableMetadata> tables = new ArrayList<>();
        if (tableNames != null) {
            for (String tableName : tableNames) {
                Table table = HiveTableInformation.getHiveTable(hiveMetastoreClient, tableName, databaseName);
                tables.add(HiveTableInformation.getTableMetadataFromHCatTableMetadata(tableName.toUpperCase(), HiveTableInformation.getHCatTableMetadata(table)));
            }
        }
        return new SchemaMetadata("", tables);
    }

    private static String handleCreateVirtualSchema(CreateVirtualSchemaRequest request, ExaMetadata meta) throws SQLException, MetadataException {
        final ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(request.getSchemaMetadataInfo().getProperties(), meta);
        List<String> tableNames = HiveAdapterProperties.getTableFilter(request.getSchemaMetadataInfo().getProperties());
        SchemaMetadata schemaMetadata = readMetadata(request.getSchemaMetadataInfo(), connection, tableNames, null);
        return ResponseJsonSerializer.makeCreateVirtualSchemaResponse(schemaMetadata);
    }

    private static String handleDropVirtualSchema() {
        return ResponseJsonSerializer.makeDropVirtualSchemaResponse();
    }

    private static String handleRefresh(RefreshRequest request, ExaMetadata meta) throws SQLException, MetadataException {
        SchemaMetadata remoteMeta;
        ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(request.getSchemaMetadataInfo().getProperties(), meta);
        if (request.isRefreshForTables()) {
            List<String> tables = request.getTables();
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), connection, tables, null);
        } else {
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), connection, null, null);
        }
        return ResponseJsonSerializer.makeRefreshResponse(remoteMeta);
    }

    public static String handleGetCapabilities() {
        Capabilities capabilities = HiveProperties.getCapabilities();
        return ResponseJsonSerializer.makeGetCapabilitiesResponse(capabilities);
    }


    private static String handlePushdownRequest(PushdownRequest request, ExaMetadata exaMeta) throws SQLException, AdapterException {
        SchemaMetadataInfo meta = request.getSchemaMetadataInfo();
        // Generate SQL pushdown query
        SqlStatementSelect selectStatement = (SqlStatementSelect) request.getSelect();
        SqlSelectList selectList = selectStatement.getSelectList();
        SqlTable fromTable = selectStatement.getFromClause();
        String tableName = fromTable.getName();
        ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(meta.getProperties(), exaMeta);

        HiveMetaStoreClient hiveMetastoreClient = HiveMetastoreService.getHiveMetastoreClient(connection.getAddress(), HiveQueryGenerator.isKerberosAuth(connection.getPassword()),connection.getUser().replaceAll("hdfs", "hive"));
        Table table = HiveTableInformation.getHiveTable(hiveMetastoreClient, tableName, HiveAdapterProperties.getSchema(meta.getProperties()));
        SqlGenerator sqlGenerator = new SqlGenerator();
        String selectListPart = selectList.accept(sqlGenerator);
        SqlGeneratorForWhereClause sqlGeneratorForWhereClause = new SqlGeneratorForWhereClause();
        String whereClause = HiveQueryGenerator.getWhereClause(selectStatement, sqlGeneratorForWhereClause);

        String outputColumnsString = "";
        String selectedColumsString = "";
        if (!sqlGenerator.loadAllColumns || !sqlGeneratorForWhereClause.loadAllColumns) {
            List<String> outputColumns = sqlGenerator.getOutputColumns();
            List<String> outputColumnsInWhere = sqlGeneratorForWhereClause.getOutputColumns();
            List<String> selectedColumns = sqlGenerator.getSelectedColumns();
            List<String> selectedColumnsInWhere = sqlGeneratorForWhereClause.getSelectedColumns();
            for (String col : outputColumnsInWhere) {
                if(!outputColumns.contains(col)) {
                    outputColumns.add(col);
                }
            }
            for (String col : selectedColumnsInWhere) {
                if(!selectedColumns.contains(col)) {
                    selectedColumns.add(col);
                }
            }
            if(selectedColumns.isEmpty()){
                for(ColumnMetadata columnMeta : fromTable.getMetadata().getColumns()){
                    try {
                        selectedColumns.add('"' + columnMeta.getName() + '"' + " " +HiveTableInformation.typeMapping(ColumnAdapterNotes.deserialize(columnMeta.getAdapterNotes(), columnMeta.getName()).getOriginalTypeName()));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            outputColumnsString = StringUtils.join(outputColumns, ",");
            selectedColumsString = StringUtils.join(selectedColumns, ",");
        }
        String partitionString = "";
        if (!sqlGeneratorForWhereClause.loadAllPartitions) {
            partitionString = StringUtils.join(sqlGeneratorForWhereClause.getSelectedPartitions(), "/");
        }


        String importSql = HiveQueryGenerator.getOutputSql(meta, table, partitionString.toLowerCase(), outputColumnsString,selectedColumsString.toUpperCase(), selectListPart.toUpperCase(), whereClause, connection);

        return ResponseJsonSerializer.makePushdownResponse(importSql);
    }


    private static String handleSetProperty(SetPropertiesRequest request, ExaMetadata exaMeta) throws SQLException, MetadataException {
        Map<String, String> changedProperties = request.getProperties();
        Map<String, String> newSchemaMeta = HiveAdapterProperties.getNewProperties(
                request.getSchemaMetadataInfo().getProperties(), changedProperties);
        if (HiveAdapterProperties.isRefreshNeeded(changedProperties)) {
            ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(newSchemaMeta, exaMeta);
            List<String> tableFilter = HiveAdapterProperties.getTableFilter(newSchemaMeta);
            String newSchema = HiveAdapterProperties.getSchema(newSchemaMeta);
            SchemaMetadata remoteMeta = readMetadata(request.getSchemaMetadataInfo(), connection, tableFilter, newSchema);
            return ResponseJsonSerializer.makeSetPropertiesResponse(remoteMeta);
        }
        return ResponseJsonSerializer.makeSetPropertiesResponse(null);
    }


}
