package com.exasol;

import org.apache.commons.lang.NotImplementedException;
import java.math.BigInteger;
import java.util.List;

public class ExaMetadataDummy implements ExaMetadata {

    private List<Class<?>> inputTypes;

    public ExaMetadataDummy(List<Class<?>> inputTypes) {
        this.inputTypes = inputTypes;
    }

    @Override
    public String getDatabaseName() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getDatabaseVersion() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getScriptLanguage() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getScriptName() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getScriptSchema() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getCurrentUser() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getCurrentSchema() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getScriptCode() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getSessionId() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getStatementId() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getNodeCount() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getNodeId() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getVmId() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public BigInteger getMemoryLimit() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getInputType() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getInputColumnCount() {
        return inputTypes.size();
    }

    @Override
    public String getInputColumnName(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Class<?> getInputColumnType(int column) throws ExaIterationException {
        return inputTypes.get(column);
    }

    @Override
    public String getInputColumnSqlType(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getInputColumnPrecision(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getInputColumnScale(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getInputColumnLength(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getOutputType() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getOutputColumnCount() {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getOutputColumnName(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Class<?> getOutputColumnType(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public String getOutputColumnSqlType(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getOutputColumnPrecision(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getOutputColumnScale(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public long getOutputColumnLength(int column) throws ExaIterationException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public Class<?> importScript(String name) throws ExaCompilationException, ClassNotFoundException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public ExaConnectionInformation getConnection(String name) throws ExaConnectionAccessException {
        throw new NotImplementedException("Not yet implemented");
    }
}
