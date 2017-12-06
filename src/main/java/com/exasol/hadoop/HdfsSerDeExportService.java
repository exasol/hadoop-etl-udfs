package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.*;
import com.exasol.hadoop.parquet.Tuple;
import com.exasol.hadoop.parquet.TupleWriteSupport;

/**
 * Writes files to (web)HDFS using the appropriate Hadoop OutputFormat and Hive SerDe.
 */
public class HdfsSerDeExportService {

    /**
     * Writes Parquet files to (web)HDFS.
     */
    public static void exportToParquetTable(
            final String hdfsUrl,
            final String hdfsUser,
            final boolean useKerberos,
            final KerberosCredentials kerberosCredentials,
            final String file,
            final HCatTableMetadata tableMeta,
            final String compressionType, // Defined in parquet.hadoop.metadata.CompressionCodecName.java
            final List<Type> schemaTypes, // Only used if 'tableMeta' is null (e.g., testing)
            final int firstColumnIndex, // First column containing data to be exported. (see ExportIntoHiveTable.java)
            final List<Integer> dynamicPartitionExaColNums, // Exasol column numbers of dynamic partitions.
            final ExaIterator ctx) throws Exception {
        System.out.println("----------\nStarted export to hive Parquet table\n----------");

        int numColumns;
        MessageType schema;
        if (tableMeta != null) {
            // Use HCat table metadata to build Parquet schema.
            // This should normally be used (except for testing).
            List<String> colNames = new ArrayList<>();
            for (HCatTableColumn col : tableMeta.getColumns()) {
                colNames.add(col.getName());
            }
            List<String> colTypeNames = new ArrayList<>();
            for (HCatTableColumn col : tableMeta.getColumns()) {
                colTypeNames.add(col.getDataType());
            }
            List<TypeInfo> colTypes = new ArrayList<>();
            for (String col : colTypeNames) {
                colTypes.add(TypeInfoFactory.getPrimitiveTypeInfo(col));
            }
            schema = HiveSchemaConverter.convert(colNames, colTypes);
            numColumns = tableMeta.getColumns().size();
        }
        else {
            // Use the schemaTypes provided since HCat table metadata isn't available.
            // This should normally only be used for testing.
            schema = new MessageType("hive_schema", schemaTypes);
            numColumns = schemaTypes.size();
        }
        System.out.println("Parquet schema:\n" + schema);

        UserGroupInformation ugi;
        if (useKerberos) {
            ugi = KerberosHadoopUtils.getKerberosUGI(kerberosCredentials);
        } else {
            ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        }
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                if (ctx.size() > 0) {
                    // Configure ParquetWriter
                    Configuration conf = new Configuration();
                    if (useKerberos) {
                        conf.set("dfs.namenode.kerberos.principal", hdfsUser);
                    }
                    TupleWriteSupport.setSchema(schema, conf);
                    Path path = new Path(hdfsUrl, file);
                    System.out.println("Path: " + path.toString());
                    ParquetWriter<Tuple> writer = new ParquetWriter<Tuple>(path,
                            new TupleWriteSupport(),
                            CompressionCodecName.fromConf(compressionType),
                            ParquetWriter.DEFAULT_BLOCK_SIZE,
                            ParquetWriter.DEFAULT_PAGE_SIZE,
                            ParquetWriter.DEFAULT_PAGE_SIZE,
                            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                            conf);

                    // Create Tuple object with ExaIterator reference.
                    // Calling 'row.next()' accesses next Exasol row to write.
                    Tuple row = new Tuple(ctx, numColumns, firstColumnIndex, dynamicPartitionExaColNums);
                    int rowsExported = 0;
                    do {
                        // Write data row
                        writer.write(row);
                        rowsExported++;
                    } while (row.next()); // Get next row
                    writer.close();
                    // Emit 'Rows affected' value
                    ctx.emit(rowsExported);
                }
                return null;
            }
        });
    }

}
