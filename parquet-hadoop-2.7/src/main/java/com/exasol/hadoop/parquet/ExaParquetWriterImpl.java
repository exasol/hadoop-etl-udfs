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
    private Tuple row;

    public ExaParquetWriterImpl(final List<String> colNames,
                                final List<TypeInfo> colTypes,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {

        super(path,
                new TupleWriteSupport(HiveSchemaConverter.convert(colNames, colTypes), conf),
                CompressionCodecName.fromConf(compressionType),
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                WriterVersion.fromString("v1"),
                conf);

        System.out.println("Path: " + path.toString());
        MessageType schema = HiveSchemaConverter.convert(colNames, colTypes);
        System.out.println("Parquet schema:\n" + schema);

        init(exa, colNames.size(), firstColumnIndex, dynamicPartitionExaColNums);
    }

    public ExaParquetWriterImpl(final List<ExaParquetTypeInfo> schemaTypes,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {


        super(path,
                new TupleWriteSupport(new MessageType("hive_schema", ExaParquetWriterImpl.toParquetTypes(schemaTypes)), conf),
                CompressionCodecName.fromConf(compressionType),
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                WriterVersion.fromString("v1"),
                conf);

        System.out.println("Path: " + path.toString());
        // Use the schemaTypes provided since HCat table metadata isn't available.
        // This should normally only be used for testing.
        MessageType schema = new MessageType("hive_schema", ExaParquetWriterImpl.toParquetTypes(schemaTypes));
        System.out.println("Parquet schema:\n" + schema);

        init(exa, schemaTypes.size(), firstColumnIndex, dynamicPartitionExaColNums);
    }

    private void init(final ExaIterator exa,
                      final int numColumns,
                      final int firstColumnIndex,
                      final List<Integer> dynamicPartitionExaColNums) throws Exception {

        // Create Tuple object with ExaIterator reference.
        this.row = new Tuple(exa, numColumns, firstColumnIndex, dynamicPartitionExaColNums);
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

    static private List<Type> toParquetTypes(final List<ExaParquetTypeInfo> exaParquetTypeInfos) {
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
