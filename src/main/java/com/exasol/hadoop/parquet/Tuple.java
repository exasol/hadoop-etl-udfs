package com.exasol.hadoop.parquet;

import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class Tuple {
    private Object[] data;
    private ExaIterator iter;

    public Tuple(ExaIterator iter, final int numCols) {
        this.iter = iter;
        data = new Object[numCols];
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

    public void writePrimitiveValue(RecordConsumer recordConsumer, int index, PrimitiveType primitiveType) {
        if (index >= data.length) {
            throw new RuntimeException("Tuple index " + index + " is out of bounds.");
        }

        String primitiveTypeName = primitiveType.getPrimitiveTypeName().toString().toUpperCase();
        if (primitiveTypeName.endsWith("BYTE_ARRAY")) {
            if (primitiveType.getDecimalMetadata() != null)
                primitiveTypeName = "DECIMAL";
            else
                primitiveTypeName = "STRING";
        }

        try {
            switch (primitiveTypeName) {
                case "INT32":
                    recordConsumer.addInteger(iter.getInteger(index));
                    break;
                case "INT64":
                    recordConsumer.addLong(iter.getLong(index));
                    break;
                case "FLOAT":
                    Float floatVal = null;
                    Double doubleVal = iter.getDouble(index);
                    if (doubleVal != null)
                        floatVal = doubleVal.floatValue();
                    recordConsumer.addFloat(floatVal);
                    break;
                case "DOUBLE":
                    recordConsumer.addDouble(iter.getDouble(index));
                    break;
                case "DECIMAL":
                    byte[] decimalBytes = null;
                    BigDecimal bigDecimalVal = iter.getBigDecimal(index);
                    if (bigDecimalVal != null)
                        decimalBytes = bigDecimalVal.unscaledValue().toByteArray();
                    recordConsumer.addBinary(Binary.fromByteArray(decimalBytes));
                    break;
                case "STRING":
                    try {
                        byte[] stringBytes = null;
                        String stringVal = iter.getString(index);
                        if (stringVal != null)
                            stringBytes = stringVal.getBytes("UTF-8");
                        recordConsumer.addBinary(Binary.fromByteArray(stringBytes));
                    } catch (UnsupportedEncodingException ex) {
                        throw new RuntimeException("writeBinaryValue String: " + ex.toString());
                    }
                    break;
                case "BOOLEAN":
                    recordConsumer.addBoolean(iter.getBoolean(index));
                    break;
                default:
                    throw new RuntimeException("Unsupported primitive type: " + primitiveTypeName);
            }
        } catch (ExaIterationException ex) {
            throw new RuntimeException("Caught ExaIterationException: " + ex.toString());
        } catch (ExaDataTypeException ex) {
            throw new RuntimeException("Caught ExaDataTypeException: " + ex.toString());
        }
    }
}
