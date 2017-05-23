package com.exasol.hadoop.parquet;

import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;

import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
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

        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
        OriginalType originalType = primitiveType.getOriginalType();
        try {
            switch (primitiveTypeName) {
                case INT32:
                    recordConsumer.addInteger(iter.getInteger(index));
                    break;
                case INT64:
                    recordConsumer.addLong(iter.getLong(index));
                    break;
                case FLOAT:
                    Float floatVal = null;
                    Double doubleVal = iter.getDouble(index);
                    if (doubleVal != null)
                        floatVal = doubleVal.floatValue();
                    recordConsumer.addFloat(floatVal);
                    break;
                case DOUBLE:
                    recordConsumer.addDouble(iter.getDouble(index));
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    if (originalType == OriginalType.DECIMAL) {
                        byte[] decimalBytes = null;
                        BigDecimal bigDecimalVal = iter.getBigDecimal(index);
                        if (bigDecimalVal != null)
                            decimalBytes = bigDecimalVal.unscaledValue().toByteArray();
                        recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBytes));
                    }
                    else
                        throw new RuntimeException("Unsupported FIXED_LEN_BYTE_ARRAY, original type: " + originalType);
                    break;
                case BINARY:
                    if (originalType == OriginalType.UTF8)
                        recordConsumer.addBinary(Binary.fromString(iter.getString(index)));
                    else
                        throw new RuntimeException("Unsupported BINARY, original type: " + originalType);
                    break;
                case BOOLEAN:
                    recordConsumer.addBoolean(iter.getBoolean(index));
                    break;
                case INT96:
                    throw new RuntimeException("Unsupported primitive type: " + primitiveTypeName);
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
