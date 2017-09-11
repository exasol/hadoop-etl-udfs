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

public class HdfsSerDeExportService {

    public static void exportToParquetTable(
            final String hdfsUrl,
            final String hdfsUser,
            final boolean useKerberos,
            final KerberosCredentials kerberosCredentials,
            final String file,
            final HCatTableMetadata tableMeta,
            final String compressionType,
            final List<Type> schemaTypes,
            final int firstColumnIndex,
            final List<Integer> dynamicPartitionExaColNums,
            final ExaIterator ctx) throws Exception {
        System.out.println("----------\nStarted Export Parquet Table To Hive\n----------");

        int numColumns;
        MessageType schema;
        if (tableMeta != null) {
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

                    Tuple row = new Tuple(ctx, numColumns, firstColumnIndex, dynamicPartitionExaColNums);
                    int rowsExported = 0;
                    do {
                        writer.write(row);
                        rowsExported++;
                    } while (row.next());
                    writer.close();
                    ctx.emit(rowsExported);
                }
                return null;
            }
        });
    }

}
