package com.exasol.hadoop.parquet;

import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.util.HashMap;

/**
 * Extends WriteSupport to support Tuple (see Tuple.java).
 */
public class TupleWriteSupport extends WriteSupport<Tuple> {
    private MessageType schema;
    private TupleWriter writer;
    private static final String PARQUET_SCHEMA_PROPERTY_NAME = "exasol.parquet.schema";

    public TupleWriteSupport() {
    }

    public static void setSchema(MessageType schema, Configuration configuration) {
        configuration.set(PARQUET_SCHEMA_PROPERTY_NAME, schema.toString());
    }

    public static MessageType getSchema(Configuration configuration) {
        return MessageTypeParser.parseMessageType(configuration.get(PARQUET_SCHEMA_PROPERTY_NAME));
    }

    public WriteContext init(Configuration configuration) {
        schema = getSchema(configuration);
        return new WriteContext(schema, new HashMap());
    }

    public void prepareForWrite(RecordConsumer recordConsumer) {
        writer = new TupleWriter(recordConsumer, schema);
    }

    public void write(Tuple tuple) {
        writer.write(tuple);
    }
}
