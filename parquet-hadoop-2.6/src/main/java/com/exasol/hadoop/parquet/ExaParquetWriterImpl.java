package com.exasol.hadoop.parquet;

import com.exasol.ExaIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExaParquetWriterImpl implements ExaParquetWriter {
    private MessageType schema;
    private int numColumns;
    private Configuration conf;
    private Path path;
    private String compressionType;
    private ExaIterator exa;
    private int firstColumnIndex;
    private List<Integer> dynamicPartitionExaColNums;
    private ParquetWriter<Tuple> writer;
    private Tuple row;

    public ExaParquetWriterImpl(final List<String> colNames,
                                final List<TypeInfo> colTypes,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {
        this.schema = HiveSchemaConverter.convert(colNames, colTypes);
        System.out.println("Parquet schema:\n" + schema);
        this.numColumns = colNames.size();

        init(conf, path, compressionType, exa, firstColumnIndex, dynamicPartitionExaColNums);
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
        this.schema = new MessageType("hive_schema", ExaParquetWriterImpl.typeInfoToParquetTypes(schemaTypes));
        System.out.println("Parquet schema:\n" + schema);
        this.numColumns = schemaTypes.size();

        init(conf, path, compressionType, exa, firstColumnIndex, dynamicPartitionExaColNums);
    }

    private void init(final Configuration conf,
                 final Path path,
                 final String compressionType,
                 final ExaIterator exa,
                 final int firstColumnIndex,
                 final List<Integer> dynamicPartitionExaColNums) throws Exception {
        this.conf = conf;
        this.path = path;
        System.out.println("Path: " + path.toString());
        this.compressionType = compressionType;
        this.exa = exa;
        this.firstColumnIndex = firstColumnIndex;
        this.dynamicPartitionExaColNums = dynamicPartitionExaColNums;

        TupleWriteSupport.setSchema(this.schema, this.conf);
        this.writer = new ParquetWriter<>(this.path,
                new TupleWriteSupport(),
                CompressionCodecName.fromConf(this.compressionType),
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                this.conf);

        // Create Tuple object with ExaIterator reference.
        this.row = new Tuple(this.exa, this.numColumns, this.firstColumnIndex, this.dynamicPartitionExaColNums);
    }

    @Override
    public void write() throws Exception {
        this.writer.write(this.row);
    }

    @Override
    public boolean next() throws Exception {
        return this.row.next();
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
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
