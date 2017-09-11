package com.exasol;

import org.apache.commons.lang.NotImplementedException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Dummy implementation for ExaIterator for writing tests without the need for an EXASOL database.
 */
public class ExaIteratorDummy implements ExaIterator {

    private List<List<Object>> inputRows;
    private List<List<Object>> outputRows = new ArrayList<>();
    int curRow = 0;

    public ExaIteratorDummy(List<List<Object>> inputRows) {
        this.inputRows = inputRows;
    }

    public List<List<Object>> getEmittedRows() {
        return outputRows;
    }

    @Override
    public long size() throws ExaIterationException {
        return inputRows.size();
    }

    @Override
    public boolean next() throws ExaIterationException {
        if (curRow + 1 < size()) {
            curRow++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void reset() throws ExaIterationException {
        curRow = 0;
    }

    @Override
    public void emit(Object... objects) throws ExaIterationException, ExaDataTypeException {
        outputRows.add(Arrays.asList(objects));
    }

    @Override
    public Integer getInteger(int i) throws ExaIterationException, ExaDataTypeException {
        return (Integer) inputRows.get(curRow).get(i);
    }

    @Override
    public Integer getInteger(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Long getLong(int i) throws ExaIterationException, ExaDataTypeException {
        return (Long) inputRows.get(curRow).get(i);
    }

    @Override
    public Long getLong(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws ExaIterationException, ExaDataTypeException {
        return (BigDecimal) inputRows.get(curRow).get(i);
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Double getDouble(int i) throws ExaIterationException, ExaDataTypeException {
        return (Double) inputRows.get(curRow).get(i);
    }

    @Override
    public Double getDouble(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getString(int i) throws ExaIterationException, ExaDataTypeException {
        return (String) inputRows.get(curRow).get(i);
    }

    @Override
    public String getString(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Boolean getBoolean(int i) throws ExaIterationException, ExaDataTypeException {
        return (Boolean) inputRows.get(curRow).get(i);
    }

    @Override
    public Boolean getBoolean(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Date getDate(int i) throws ExaIterationException, ExaDataTypeException {
        return (Date) inputRows.get(curRow).get(i);
    }

    @Override
    public Date getDate(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Timestamp getTimestamp(int i) throws ExaIterationException, ExaDataTypeException {
        return (Timestamp) inputRows.get(curRow).get(i);
    }

    @Override
    public Timestamp getTimestamp(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Object getObject(int i) throws ExaIterationException, ExaDataTypeException {
        return inputRows.get(curRow).get(i);
    }

    @Override
    public Object getObject(String s) throws ExaIterationException, ExaDataTypeException {
        throw new NotImplementedException("Not yet implemented");
    }
}
