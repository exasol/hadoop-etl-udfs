package com.exasol.adapter;

import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlGeneratorForWhereClause extends SqlGenerator {


    private Set<String> outputColumns;
    private Set<String> selectedPartitions;
    private Set<String> selectedColumns;

    public Boolean loadAllColumns;
    public Boolean loadAllPartitions;

    public SqlGeneratorForWhereClause() {
        this.loadAllPartitions = false;
        this.loadAllColumns = false;
        outputColumns = new HashSet<>();
        selectedPartitions = new HashSet<>();
        selectedColumns = new HashSet<>();
    }

    public Set<String> getOutputColumns(){
        return outputColumns;
    }

    public Set<String> getSelectedColumns(){
        return selectedColumns;
    }

    public Set<String> getSelectedPartitions(){
        return selectedPartitions;
    }



    @Override
    public String visit(SqlSelectList selectList) throws AdapterException {
        List<String> selectElement = new ArrayList<>();
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            selectElement.add("true");
        } else if (selectList.isSelectStar()) {
            selectElement.add("*");
        } else {
            for (SqlNode node : selectList.getExpressions()) {
                selectElement.add(node.accept(this));
            }
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public String visit(SqlColumn column) {
        if(!this.loadAllColumns) {
            outputColumns.add(column.getName());
        }
            try {
                selectedColumns.add(column.getName() + " " +HiveTableInformation.typeMapping(column.getType().toString()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        return column.getName();
    }

    @Override
    public String visit(SqlPredicateEqual predicate) throws AdapterException {

        SqlNode leftNode = predicate.getLeft();
        SqlNode rightNode = predicate.getRight();
        if(!loadAllPartitions){
            if(leftNode.getType().equals(SqlNodeType.COLUMN) && rightNode.getType().equals(SqlNodeType.LITERAL_STRING)){
                SqlColumn sqlColumn = (SqlColumn) leftNode;
                boolean isPartitioned = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).isPartitionedColumn();
                if(isPartitioned){
                    selectedPartitions.add(sqlColumn.getName()+"=" + ((SqlLiteralString) rightNode).getValue());
                }
                else{
                    loadAllPartitions = true;
                }
            }
            else if(rightNode.getType().equals(SqlNodeType.COLUMN) && leftNode.getType().equals(SqlNodeType.LITERAL_STRING)){
                SqlColumn sqlColumn = (SqlColumn) rightNode;
                boolean isPartitioned = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).isPartitionedColumn();
                if(isPartitioned){
                    selectedPartitions.add(sqlColumn.getName() +"=" + ((SqlLiteralString) leftNode).getValue());
                }
                else{
                    loadAllPartitions = true;
                }
            }
        }
        return leftNode.accept(this) + " = "
                + rightNode.accept(this);
    }

    @Override
    public String visit(SqlPredicateInConstList predicate) throws AdapterException {
        List<String> argumentsSql = new ArrayList<>();
        List<String> argumentsSqlOnlyValue = new ArrayList<>();
        boolean isListOfStrings = true;
        for (SqlNode node : predicate.getInArguments()) {
            if(!node.getType().equals(SqlNodeType.LITERAL_STRING)){
                isListOfStrings = false;
            }
           else {
                argumentsSqlOnlyValue.add(((SqlLiteralString) node).getValue());
            }
            argumentsSql.add(node.accept(this));
        }
        SqlNode expression = predicate.getExpression();
        if(isListOfStrings && !loadAllPartitions) {
            if (expression.getType().equals(SqlNodeType.COLUMN)) {
                SqlColumn sqlColumn = (SqlColumn) expression;
                boolean isPartitioned = ColumnAdapterNotes.deserialize(sqlColumn.getMetadata().getAdapterNotes(), sqlColumn.getMetadata().getName()).isPartitionedColumn();
                if(isPartitioned) {
                    for (String argument : argumentsSqlOnlyValue) {
                       selectedPartitions.add(sqlColumn.getName() + "=" + argument);
                    }
                }
                else{
                    loadAllPartitions = true;
                }
            }
        }
        return expression.accept(this) + " IN ("
                + Joiner.on(", ").join(argumentsSql) + ")";
    }


}
