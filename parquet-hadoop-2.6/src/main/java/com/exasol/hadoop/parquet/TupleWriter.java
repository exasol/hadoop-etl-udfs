package com.exasol.hadoop.parquet;

import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

/**
 * Writes Tuple (see Tuple.java) fields to RecordConsumer for TupleWriteSupport (TupleWriteSupport.java).
 */
public class TupleWriter {
    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public TupleWriter(RecordConsumer recordConsumer, GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public void write(Tuple tuple) {
        recordConsumer.startMessage();
        writeTuple(tuple, schema);
        recordConsumer.endMessage();
    }

    private void writeTuple(Tuple tuple, GroupType type) {
        for (int index = 0; index < type.getFieldCount(); index++) {
            Type fieldType = type.getType(index);
            String fieldName = fieldType.getName();
            // empty fields have to be omitted
            if (tuple.isNull(index))
                continue;
            recordConsumer.startField(fieldName, index);
            if (fieldType.isPrimitive()) {
                tuple.writePrimitiveValue(recordConsumer, index, (PrimitiveType)fieldType);
            }
            else {
                recordConsumer.startGroup();
                writeTuple(tuple.getTuple(index), fieldType.asGroupType());
                recordConsumer.endGroup();
            }
            recordConsumer.endField(fieldName, index);
        }
    }
}
