package com.exasol.hadoop.parquet;

import com.exasol.ExaIterator;
import com.exasol.hadoop.hcat.HCatTableColumn;
import com.exasol.hadoop.hcat.HCatTableMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

public class ExaParquetWriterImpl implements ExaParquetWriter {
    private MessageType schema;
    private int numColumns;
    Configuration conf;
    Path path;
    String compressionType;
    ExaIterator exa;
    int firstColumnIndex;
    List<Integer> dynamicPartitionExaColNums;
    ParquetWriter<Tuple> writer;
    Tuple row;

    public ExaParquetWriterImpl(final HCatTableMetadata tableMeta,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {
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
        this.schema = HiveSchemaConverter.convert(colNames, colTypes);
        System.out.println("Parquet schema:\n" + schema);
        this.numColumns = tableMeta.getColumns().size();

        init(conf, path, compressionType, exa, firstColumnIndex, dynamicPartitionExaColNums);
    }

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

    public ExaParquetWriterImpl(final List<Type> schemaTypes,
                                final Configuration conf,
                                final Path path,
                                final String compressionType,
                                final ExaIterator exa,
                                final int firstColumnIndex,
                                final List<Integer> dynamicPartitionExaColNums) throws Exception {
        // Use the schemaTypes provided since HCat table metadata isn't available.
        // This should normally only be used for testing.
        this.schema = new MessageType("hive_schema", schemaTypes);
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

    public void write() throws Exception {
        this.writer.write(this.row);
    }

    public boolean next() throws Exception {
        return this.row.next();
    }

    public void close() throws Exception {
        this.writer.close();
    }
}
