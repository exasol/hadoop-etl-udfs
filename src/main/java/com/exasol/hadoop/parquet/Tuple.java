package com.exasol.hadoop.parquet;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;

public class Tuple {
    private final GroupType schema;
    private final Object[] data;

    public Tuple(GroupType schema) {
        this.schema = schema;
        data = new Object[this.schema.getFields().size()];
    }

    public void setValue(int index, Object obj) {
        if (index >= data.length) {
            throw new RuntimeException("Tuple index " + index + " is out of bounds.");
        }
        data[index] = obj;
    }

    public Tuple getTuple(int index) {
        if (index >= data.length) {
            throw new RuntimeException("Tuple index " + index + " is out of bounds.");
        }
        if (data[index] instanceof Tuple) {
            return (Tuple)data[index];
        }
        else {
            throw new RuntimeException("Object at index " + index + " is of type " + data[index].getClass().getName() + ", not of type Tuple");
        }
    }

    public void writeValue(int index, RecordConsumer recordConsumer) {
        if (index >= data.length) {
            throw new RuntimeException("Tuple index " + index + " is out of bounds.");
        }
        Object obj = data[index];
        if (obj instanceof Integer) {
            recordConsumer.addInteger((Integer)obj);
        }
        else if (obj instanceof Long) {
            recordConsumer.addLong((Long)obj);
        }
        else if (obj instanceof Float) {
            recordConsumer.addFloat((Float)obj);
        }
        else if (obj instanceof Double) {
            recordConsumer.addDouble((Double)obj);
        }
        else if (obj instanceof String) {
            recordConsumer.addBinary(Binary.fromString((String)obj));
        }
        else if (obj instanceof Boolean) {
            recordConsumer.addBoolean((Boolean)obj);
        }
        else {
            throw new RuntimeException("Tuple.writeValue(): Unknown object type: " + obj.getClass().getName());
        }
    }
}
