package com.exasol.hadoop;

import com.exasol.ExaIterator;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import com.exasol.hadoop.parquet.ExaParquetWriter;
import com.exasol.hadoop.parquet.ExaParquetWriterImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import parquet.schema.Type;

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
            final ExaIterator ctx,
            final List<String> colNames,
            final List<TypeInfo> colTypes) throws Exception {
        System.out.println("----------\nStarted export to hive Parquet table\n----------");

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
                    Path path = new Path(hdfsUrl, file);
                    int rowsExported = 0;

                    ExaParquetWriter parquetWriter;
                    if (tableMeta != null) {
                        // Use HCat table metadata to build Parquet schema.
                        // This should normally be used (except for testing).
                        List<String> metaColNames = new ArrayList<>();
                        for (HCatTableColumn col : tableMeta.getColumns()) {
                            metaColNames.add(col.getName());
                        }
                        List<String> metaColTypeNames = new ArrayList<>();
                        for (HCatTableColumn col : tableMeta.getColumns()) {
                            metaColTypeNames.add(col.getDataType());
                        }
                        List<TypeInfo> metaColTypes = new ArrayList<>();
                        for (String col : metaColTypeNames) {
                            metaColTypes.add(TypeInfoFactory.getPrimitiveTypeInfo(col));
                        }
                        parquetWriter = new ExaParquetWriterImpl(metaColNames, metaColTypes, conf, path, compressionType, ctx, firstColumnIndex, dynamicPartitionExaColNums);
                    } else if (colNames != null && colTypes != null) {
                        parquetWriter = new ExaParquetWriterImpl(colNames, colTypes, conf, path, compressionType, ctx, firstColumnIndex, dynamicPartitionExaColNums);
                    } else {
                        parquetWriter = new ExaParquetWriterImpl(schemaTypes, conf, path, compressionType, ctx, firstColumnIndex, dynamicPartitionExaColNums);
                    }

                    do {
                        // Write data row
                        parquetWriter.write();
                        rowsExported++;
                    } while (parquetWriter.next()); // Get next row
                    parquetWriter.close();

                    // Emit 'Rows affected' value
                    ctx.emit(rowsExported);
                }
                return null;
            }
        });
    }

}
