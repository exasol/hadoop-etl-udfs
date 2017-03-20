package com.exasol.adapter;

import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlGenerator implements SqlNodeVisitor<String> {

    private Set<String> outputColumns;
    private Set<String> selectedColumns;

    public Boolean loadAllColumns;

    public SqlGenerator() {
        outputColumns = new HashSet<>();
        selectedColumns = new HashSet<>();
        this.loadAllColumns = false;
    }

    public Set<String> getOutputColumns(){
        return outputColumns;
    }

    public Set<String> getSelectedColumns(){
        return selectedColumns;
    }

    @Override
    public String visit(SqlSelectList selectList) {
        List<String> selectElement = new ArrayList<>();
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            selectElement.add("true");
        } else if (selectList.isSelectStar()) {
            selectElement.add("*");
            loadAllColumns = true;
        } else {
            for (SqlNode node : selectList.getExpressions()) {
                selectElement.add(node.accept(this));
            }
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public String visit(SqlColumn column) {
         if(!loadAllColumns) {
             outputColumns.add(column.getName());
         }
             try {
                 selectedColumns.add('"' + column.getName() + '"' + " " +HiveTableInformation.typeMapping(ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getOriginalTypeName()));
             } catch (SQLException e) {
                 e.printStackTrace();
             }
         return '"' + column.getName() + '"';
    }

    @Override
    public String visit(SqlLiteralBool literal) {
        if (literal.getValue()) {
            return "true";
        } else {
            return "false";
        }
    }

    @Override
    public String visit(SqlLiteralDate literal) {
        return "DATE '" + literal.getValue() + "'"; // This gets always executed
        // as
        // TO_DATE('2015-02-01','YYYY-MM-DD')
    }

    @Override
    public String visit(SqlLiteralDouble literal) {
        return Double.toString(literal.getValue());
    }

    @Override
    public String visit(SqlLiteralExactnumeric literal) {
        return literal.getValue().toString();
    }

    @Override
    public String visit(SqlLiteralNull literal) {
        return "NULL";
    }

    @Override
    public String visit(SqlLiteralString literal) {
        return HiveProperties.getStringLiteral(literal.getValue());
    }

    @Override
    public String visit(SqlLiteralTimestamp literal) {
        // TODO Allow dialect to modify behavior
        return "TIMESTAMP '" + literal.getValue().toString() + "'";
    }

    @Override
    public String visit(SqlLiteralTimestampUtc literal) {
        // TODO Allow dialect to modify behavior
        return "TIMESTAMP '" + literal.getValue().toString() + "'";
    }

    @Override
    public String visit(SqlLiteralInterval literal) {
        // TODO Allow dialect to modify behavior
        if (literal.getDataType().getIntervalType() == DataType.IntervalType.YEAR_TO_MONTH) {
            return "INTERVAL '" + literal.getValue().toString()
                    + "' YEAR (" + literal.getDataType().getPrecision() + ") TO MONTH";
        } else {
            return "INTERVAL '" + literal.getValue().toString() + "' DAY (" + literal.getDataType().getPrecision()
                    + ") TO SECOND (" + literal.getDataType().getIntervalFraction() + ")";
        }
    }

    @Override
    public String visit(SqlPredicateEqual predicate) {
        SqlNode leftNode = predicate.getLeft();
        SqlNode rightNode = predicate.getRight();
        return leftNode.accept(this) + " = "
                + rightNode.accept(this);
    }

    @Override
    public String visit(SqlPredicateInConstList predicate) {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : predicate.getInArguments()) {
            argumentsSql.add(node.accept(this));
        }
        return predicate.getExpression().accept(this) + " IN ("
                + Joiner.on(", ").join(argumentsSql) + ")";
    }



    @Override
    public String visit(SqlStatementSelect select) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlTable table) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlGroupBy groupBy) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlFunctionAggregate function) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlFunctionAggregateGroupConcat function) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlFunctionScalar function) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlFunctionScalarCase function) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlFunctionScalarCast function) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlFunctionScalarExtract function) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlLimit limit) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlOrderBy orderBy) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateAnd predicate) {
        // Could be supported in future
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateOr predicate) {
        // could be supported in future
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateBetween predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateLess predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateLessEqual predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateLike predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateLikeRegexp predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateNot predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateNotEqual predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateIsNull predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

    @Override
    public String visit(SqlPredicateIsNotNull predicate) {
        throw new RuntimeException("Internal error: This should never be called");
    }

}

