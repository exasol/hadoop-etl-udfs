package com.exasol.hadoop.parquet;

import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.util.HashMap;

public class TupleWriteSupport extends WriteSupport<Tuple> {
    private MessageType schema;
    private TupleWriter writer;

    public TupleWriteSupport() {
    }

    public static void setSchema(MessageType schema, Configuration configuration) {
        configuration.set("exasol.parquet.schema", schema.toString());
    }

    public static MessageType getSchema(Configuration configuration) {
        return MessageTypeParser.parseMessageType(configuration.get("exasol.parquet.schema"));
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
