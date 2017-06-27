package com.exasol.hadoop.parquet;

import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;

import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.JulianFields;
import java.util.TimeZone;

public class Tuple {
    private Object[] data;
    private ExaIterator iter;
    private int firstColumnIndex;

    public Tuple(ExaIterator iter, final int numCols, final int firstColumnIndex) {
        this.iter = iter;
        data = new Object[numCols];
        this.firstColumnIndex = firstColumnIndex;
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

        int iterIndex = firstColumnIndex + index;
        PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
        OriginalType originalType = primitiveType.getOriginalType();
        try {
            switch (primitiveTypeName) {
                case INT32:
                    recordConsumer.addInteger(iter.getInteger(iterIndex));
                    break;
                case INT64:
                    recordConsumer.addLong(iter.getLong(iterIndex));
                    break;
                case FLOAT:
                    Float floatVal = null;
                    Double doubleVal = iter.getDouble(iterIndex);
                    if (doubleVal != null)
                        floatVal = doubleVal.floatValue();
                    recordConsumer.addFloat(floatVal);
                    break;
                case DOUBLE:
                    recordConsumer.addDouble(iter.getDouble(iterIndex));
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    if (originalType == OriginalType.DECIMAL) {
                        byte[] decimalBytes = null;
                        BigDecimal bigDecimalVal = iter.getBigDecimal(iterIndex);
                        if (bigDecimalVal != null)
                            decimalBytes = bigDecimalVal.unscaledValue().toByteArray();
                        recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBytes));
                    }
                    else
                        throw new RuntimeException("Unsupported FIXED_LEN_BYTE_ARRAY, original type: " + originalType);
                    break;
                case BINARY:
                    recordConsumer.addBinary(Binary.fromString(iter.getString(iterIndex)));
                    break;
                case BOOLEAN:
                    recordConsumer.addBoolean(iter.getBoolean(iterIndex));
                    break;
                case INT96:
                    // Timestamp
                    // First 8 bytes: nanoseconds within day
                    // Next 4 bytes: Julian day
                    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                    Timestamp timestamp = iter.getTimestamp(iterIndex);
                    if (timestamp != null) {
                        LocalDateTime localDateTime = timestamp.toLocalDateTime();
                        long dayNanoSeconds = localDateTime.getNano()
                                + ((localDateTime.getHour() * 60 * 60)
                                + (localDateTime.getMinute() * 60)
                                + localDateTime.getSecond()) * 1000000000L;
                        long julianDay = JulianFields.JULIAN_DAY.getFrom(timestamp.toLocalDateTime());
                        NanoTime nanoTime = new NanoTime((int)julianDay, dayNanoSeconds);
                        recordConsumer.addBinary(nanoTime.toBinary());
                    }
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
