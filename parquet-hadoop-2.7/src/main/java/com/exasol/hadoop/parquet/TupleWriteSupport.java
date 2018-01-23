package com.exasol.hadoop.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Extends WriteSupport to support Tuple (see Tuple.java).
 */
public class TupleWriteSupport extends WriteSupport<Tuple> {
    private MessageType schema;
    private TupleWriter writer;
    private static final String PARQUET_SCHEMA_PROPERTY_NAME = "exasol.parquet.schema";

    public TupleWriteSupport(MessageType schema, Configuration configuration) {
        setSchema(schema, configuration);
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
