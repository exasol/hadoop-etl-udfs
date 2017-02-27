package com.exasol.adapter;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.request.*;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;
import com.exasol.utils.UdfUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by np on 1/24/2017.
 */
public class HiveAdapter {

    public static String adapterCall(ExaMetadata meta, String input) throws Exception {
        String result = "";
        try {
            AdapterRequest request = new RequestJsonParser().parseRequest(input);
            final ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(request.getSchemaMetadataInfo().getProperties(), meta);

            switch (request.getType()) {
                case CREATE_VIRTUAL_SCHEMA:
                    CreateVirtualSchemaRequest virtualSchemaRequest = (CreateVirtualSchemaRequest) request;
                    SchemaMetadata schemaMetadata = readMetadata(virtualSchemaRequest.getSchemaMetadataInfo(), connection, null,null);
                    result = ResponseJsonSerializer.makeCreateVirtualSchemaResponse(schemaMetadata);
                    break;
                case DROP_VIRTUAL_SCHEMA:
                    result = handleDropVirtualSchema();
                    break;

                case SET_PROPERTIES:
                    result = handleSetProperty((SetPropertiesRequest)request, meta);
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
        } catch (Exception ex) {
            String stacktrace = UdfUtils.traceToString(ex);
            throw new Exception("Error in Adapter: " + ex.getMessage() + "\nStacktrace: " + stacktrace + "\nFor following request: " + input + "\nResponse: " + result);
        }
    }

    static SchemaMetadata readMetadata(SchemaMetadataInfo schemaMetaInfo, ExaConnectionInformation connection, List<String> tableNames,String newSchema) throws SQLException {
        String databaseName = newSchema;
       if(newSchema==null) {
          databaseName = HiveAdapterProperties.getSchema(schemaMetaInfo.getProperties());
       }
        HiveMetaStoreClient hiveMetastoreClient = HiveAdapterUtils.getHiveMetastoreClient(connection);
        if (tableNames == null) {
            try {
                tableNames = hiveMetastoreClient.getAllTables(databaseName);
            } catch (MetaException e) {
                e.getMessage();
            }
        }
        List<TableMetadata> tables = new ArrayList<>();
        if(tableNames!=null) {
            for (String tableName : tableNames) {
                Table table = HiveAdapterUtils.getHiveTable(hiveMetastoreClient, tableName, databaseName);
                tables.add(HiveAdapterUtils.getTableMetadataFromHCatTableMetadata(tableName.toUpperCase(), HiveAdapterUtils.getHCatTableMetadata(table)));
            }
        }
        return new SchemaMetadata("", tables);
    }

    private static String handleDropVirtualSchema() {
        return ResponseJsonSerializer.makeDropVirtualSchemaResponse();
    }

    private static String handleRefresh(RefreshRequest request, ExaMetadata meta) throws SQLException {
        SchemaMetadata remoteMeta;
        ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(request.getSchemaMetadataInfo().getProperties(), meta);
        if (request.isRefreshForTables()) {
            List<String> tables = request.getTables();
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), connection, tables,null);
        } else {
            remoteMeta = readMetadata(request.getSchemaMetadataInfo(), connection, null,null);
        }
        return ResponseJsonSerializer.makeRefreshResponse(remoteMeta);
    }

    public static String handleGetCapabilities() {
        Capabilities capabilities = HiveProperties.getCapabilities();
        return ResponseJsonSerializer.makeGetCapabilitiesResponse(capabilities);
    }


    private static String handlePushdownRequest(PushdownRequest request, ExaMetadata exaMeta) throws SQLException {
        SchemaMetadataInfo meta = request.getSchemaMetadataInfo();
        // Generate SQL pushdown query
        SqlGenerationContext context = new SqlGenerationContext(HiveAdapterProperties.getCatalog(meta.getProperties()), HiveAdapterProperties.getSchema(meta.getProperties()), HiveAdapterProperties.isLocal(meta.getProperties()));
        SqlStatementSelect selectStatement = (SqlStatementSelect) request.getSelect();
        SqlSelectList selectList = selectStatement.getSelectList();
        SqlTable fromTable = selectStatement.getFromClause();
        String tableName = fromTable.getName();
        ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(meta.getProperties(), exaMeta);

        HiveMetaStoreClient hiveMetastoreClient = HiveAdapterUtils.getHiveMetastoreClient(connection);
        Table table = HiveAdapterUtils.getHiveTable(hiveMetastoreClient, tableName, HiveAdapterProperties.getSchema(meta.getProperties()));
        List<FieldSchema> partitions = table.getPartitionKeys();
        List<String> partitionColumns = new ArrayList<>();
        for (FieldSchema partition : partitions) {
            partitionColumns.add(partition.getName());
        }

        SqlGenerator sqlGenerator = new SqlGenerator(context);
        String selectPart = selectList.accept(sqlGenerator);
        SqlGeneratorForWhereClause sqlGeneratorForWhereClause = new SqlGeneratorForWhereClause(context,partitionColumns);
        String secondPartOfQuery = HiveAdapterUtils.getSecondPartOfStatement(selectStatement,sqlGenerator,sqlGeneratorForWhereClause);

        String selectedColumnsString="";
        if(!sqlGenerator.loadAllColumns || !sqlGeneratorForWhereClause.loadAllColumns){
            Set<String> selectedColumns = sqlGenerator.getSelectedColumns();
            Set<String> selectedColumnsInWhere = sqlGeneratorForWhereClause.getSelectedColumns();
            for(String col: selectedColumnsInWhere){
                selectedColumns.add(col);
            }

           selectedColumnsString = StringUtils.join(selectedColumns, ",");
        }
        String partitionString = "";
        if(!sqlGeneratorForWhereClause.loadAllPartitions) {
            partitionString = StringUtils.join(sqlGeneratorForWhereClause.getSelectedPartitions(), "/");
        }


        String importSql = HiveAdapterUtils.getOutputSql(meta, table, partitionString, selectedColumnsString, selectPart,secondPartOfQuery, connection);

        return ResponseJsonSerializer.makePushdownResponse(importSql);
    }


    private static String handleSetProperty(SetPropertiesRequest request, ExaMetadata exaMeta) throws SQLException {
        Map<String, String> changedProperties = request.getProperties();
        Map<String, String> newSchemaMeta = HiveAdapterProperties.getNewProperties(
                request.getSchemaMetadataInfo().getProperties(), changedProperties);
        if (HiveAdapterProperties.isRefreshNeeded(changedProperties)) {
            ExaConnectionInformation connection = HiveAdapterProperties.getConnectionInformation(newSchemaMeta, exaMeta);
            List<String> tableFilter = HiveAdapterProperties.getTableFilter(newSchemaMeta);
            String newSchema = HiveAdapterProperties.getSchema(newSchemaMeta);
            SchemaMetadata remoteMeta = readMetadata(request.getSchemaMetadataInfo(),connection,tableFilter,newSchema);
            return ResponseJsonSerializer.makeSetPropertiesResponse(remoteMeta);
        }
        return ResponseJsonSerializer.makeSetPropertiesResponse(null);
    }


}
