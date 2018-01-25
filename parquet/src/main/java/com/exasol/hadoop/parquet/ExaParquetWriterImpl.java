package com.exasol.hadoop.parquet;

import com.exasol.ExaIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExaParquetWriterImpl extends ParquetWriter<Tuple> implements ExaParquetWriter {

    // There is not a default value for the Parquet version enum.
    // Use v1 for compatibility reasons.
    static final WriterVersion PARQUET_WRITER_VERSION = WriterVersion.PARQUET_1_0;

    private Tuple row;

    private ExaParquetWriterImpl(final MessageType schema,
                                 final int numColumns,
                                 final Configuration conf,
                                 final Path path,
                                 final String compressionType,
                                 final ExaIterator exa,
                                 final int firstColumnIndex,
                                 final List<Integer> dynamicPartitionExaColNums) throws Exception {
        super(path,
                new TupleWriteSupport(schema, conf),
                CompressionCodecName.fromConf(compressionType),
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                PARQUET_WRITER_VERSION,
                conf);

        System.out.println("Path: " + path.toString());
        System.out.println("Parquet schema:\n" + schema);

        // Create Tuple object with ExaIterator reference.
        this.row = new Tuple(exa, numColumns, firstColumnIndex, dynamicPartitionExaColNums);
    }

    public ExaParquetWriterImpl(final List<String> colNames,
                                final List<TypeInfo> colTypes,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {

        this(HiveSchemaConverter.convert(colNames, colTypes),
                colNames.size(),
                conf,
                path,
                compressionType,
                exa,
                firstColumnIndex,
                dynamicPartitionExaColNums);
    }

    public ExaParquetWriterImpl(final List<ExaParquetTypeInfo> schemaTypes,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {

        // Use the schemaTypes provided since HCat table metadata isn't available.
        // This should normally only be used for testing.
        this(new MessageType("hive_schema", ExaParquetWriterImpl.typeInfoToParquetTypes(schemaTypes)),
                schemaTypes.size(),
                conf,
                path,
                compressionType,
                exa,
                firstColumnIndex,
                dynamicPartitionExaColNums);
    }

    @Override
    public void write() throws Exception {
        super.write(this.row);
    }

    @Override
    public boolean next() throws Exception {
        return this.row.next();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    static private List<Type> typeInfoToParquetTypes(final List<ExaParquetTypeInfo> exaParquetTypeInfos) {
        List<Type> types = new ArrayList<>();
        for (ExaParquetTypeInfo exaType: exaParquetTypeInfos) {
            if (exaType.length != 0) {
                types.add(new PrimitiveType(
                        Type.Repetition.valueOf(exaType.typeRepitition),
                        PrimitiveType.PrimitiveTypeName.valueOf(exaType.primitiveTypeName),
                        exaType.length,
                        exaType.name));
            } else {
                types.add(new PrimitiveType(
                        Type.Repetition.valueOf(exaType.typeRepitition),
                        PrimitiveType.PrimitiveTypeName.valueOf(exaType.primitiveTypeName),
                        exaType.name,
                        exaType.originalType == null ? null : OriginalType.valueOf(exaType.originalType)));
            }
        }
        return types;
    }

}
