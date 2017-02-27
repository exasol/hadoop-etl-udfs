package com.exasol.adapter;

import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by np on 2/16/2017.
 */
public class SqlGeneratorForWhereClause implements SqlNodeVisitor<String> {

    private SqlGenerationContext context;
    private List<String> partitionColumns;
    private Set<String> selectedColumns;
    private Set<String> selectedPartitions;
    private Integer numberOfColumns;
    private Integer numberOfPartitionColumns;

    public Boolean loadAllColumns;
    public Boolean loadAllPartitions;




    public SqlGeneratorForWhereClause(SqlGenerationContext context,List<String> allPartitionColumns) {
        this.context = context;
        if(allPartitionColumns.isEmpty()){
            this.loadAllPartitions = true;
        }
        else {
            this.partitionColumns.addAll(allPartitionColumns);
            this.loadAllPartitions = false;
        }
        this.loadAllColumns = false;
        selectedColumns = new HashSet<>();
        selectedPartitions = new HashSet<>();
        numberOfColumns = 0;
        numberOfPartitionColumns = 0;
    }

    public Set<String> getSelectedColumns(){
        return selectedColumns;
    }
    public Set<String> getSelectedPartitions(){
        return selectedPartitions;
    }

    public Integer getNumberOfColumns(){
        return numberOfColumns;
    }

    public Integer getNumberOfPartitionColumns(){
        return numberOfPartitionColumns;
    }


    @Override
    public String visit(SqlStatementSelect select) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(select.getSelectList().accept(this));
        sql.append(" FROM ");
        sql.append(select.getFromClause().accept(this));
        if (select.hasFilter()) {
            sql.append(" WHERE ");
            sql.append(select.getWhereClause().accept(this));
        }
        if (select.hasGroupBy()) {
            sql.append(" GROUP BY ");
            sql.append(select.getGroupBy().accept(this));
        }
        if (select.hasHaving()) {
            sql.append(" HAVING ");
            sql.append(select.getHaving().accept(this));
        }
        if (select.hasOrderBy()) {
            sql.append(" ");
            sql.append(select.getOrderBy().accept(this));
        }
        if (select.hasLimit()) {
            sql.append(" ");
            sql.append(select.getLimit().accept(this));
        }
        return sql.toString();
    }

    @Override
    public String visit(SqlSelectList selectList) {
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
        if(!this.loadAllColumns){
            selectedColumns.add(column.getName());
        }
        numberOfColumns++;
        return "`" + column.getName() + "`";
    }

    @Override
    public String visit(SqlTable table) {
        return  "`" + context.getSchemaName() + "`"
                + "." + "`" + table.getName() + "`" ;
    }

    @Override
    public String visit(SqlGroupBy groupBy) {
        if (groupBy.getExpressions() == null || groupBy.getExpressions().isEmpty()) {
            throw new RuntimeException(
                    "Unexpected internal state (empty group by)");
        }
        List<String> selectElement = new ArrayList<>();
        for (SqlNode node : groupBy.getExpressions()) {
            selectElement.add(node.accept(this));
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public String visit(SqlFunctionAggregate function) {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        if (function.getFunctionName().equalsIgnoreCase("count") && argumentsSql.size() == 0) {
            argumentsSql.add("*");
            this.loadAllColumns = true;
            this.loadAllPartitions = true;
        }
        String distinctSql = "";
        if (function.hasDistinct()) {
            distinctSql = "DISTINCT ";
        }
        String functionNameInSourceSystem = function.getFunctionName();
        if (HiveProperties.getAggregateFunctionAliases().containsKey(function.getFunction())) {
            functionNameInSourceSystem = HiveProperties.getAggregateFunctionAliases().get(function.getFunction());
        }
        return functionNameInSourceSystem + "(" + distinctSql
                + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public String visit(SqlFunctionAggregateGroupConcat function) {
        StringBuilder builder = new StringBuilder();
        builder.append(function.getFunctionName());
        builder.append("(");
        if (function.hasDistinct()) {
            builder.append("DISTINCT ");
        }
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        builder.append(function.getArguments().get(0).accept(this));
        if (function.hasOrderBy()) {
            builder.append(" ");
            String orderByString = function.getOrderBy().accept(this);
            builder.append(orderByString);
        }
        if (function.getSeparator() != null) {
            builder.append(" SEPARATOR ");
            builder.append("'");
            builder.append(function.getSeparator());
            builder.append("'");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(SqlFunctionScalar function) {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        String functionNameInSourceSystem = function.getFunctionName();
        if (HiveProperties.getScalarFunctionAliases().containsKey(function.getFunction())) {
            // Take alias if one is defined - will overwrite the infix
            functionNameInSourceSystem = HiveProperties.getScalarFunctionAliases().get(function.getFunction());
        } else {
            if (HiveProperties.getBinaryInfixFunctionAliases().containsKey(function.getFunction())) {
                assert (argumentsSql.size() == 2);
                String realFunctionName = function.getFunctionName();
                if (HiveProperties.getBinaryInfixFunctionAliases().containsKey(function.getFunction())) {
                    realFunctionName = HiveProperties.getBinaryInfixFunctionAliases().get(function.getFunction());
                }
                return "(" + argumentsSql.get(0) + " " + realFunctionName + " "
                        + argumentsSql.get(1) + ")";
            } else if (HiveProperties.getPrefixFunctionAliases().containsKey(function.getFunction())) {
                assert (argumentsSql.size() == 1);
                String realFunctionName = function.getFunctionName();
                if (HiveProperties.getPrefixFunctionAliases().containsKey(function.getFunction())) {
                    realFunctionName = HiveProperties.getPrefixFunctionAliases().get(function.getFunction());
                }
                return "(" + realFunctionName
                        + argumentsSql.get(0) + ")";
            }
        }
        if (argumentsSql.size() == 0 && HiveProperties.omitParentheses(function.getFunction())) {
            return functionNameInSourceSystem;
        } else {
            return functionNameInSourceSystem + "(" + Joiner.on(", ").join(argumentsSql) + ")";
        }
    }

    @Override
    public String visit(SqlFunctionScalarCase function) {
        StringBuilder builder = new StringBuilder();
        builder.append("CASE");
        if (function.getBasis() != null) {
            builder.append(" ");
            builder.append(function.getBasis().accept(this));
        }
        for (int i = 0; i < function.getArguments().size(); i++) {
            SqlNode node = function.getArguments().get(i);
            SqlNode result = function.getResults().get(i);
            builder.append(" WHEN ");
            builder.append(node.accept(this));
            builder.append(" THEN ");
            builder.append(result.accept(this));
        }
        if (function.getResults().size() > function.getArguments().size()) {
            builder.append(" ELSE ");
            builder.append(function.getResults().get(function.getResults().size() - 1).accept(this));
        }
        builder.append(" END");
        return builder.toString();
    }

    @Override
    public String visit(SqlFunctionScalarCast function) {

        StringBuilder builder = new StringBuilder();
        builder.append("CAST");
        builder.append("(");
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        builder.append(function.getArguments().get(0).accept(this));
        builder.append(" AS ");
        builder.append(function.getDataType());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(SqlFunctionScalarExtract function) {
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        String expression = function.getArguments().get(0).accept(this);
        return function.getFunctionName() + "(" + function.getToExtract() + " FROM "+ expression + ")";
    }

    @Override
    public String visit(SqlLimit limit) {
        String offsetSql = "";
        if (limit.getOffset() != 0) {
            offsetSql = " OFFSET " + limit.getOffset();
        }
        return "LIMIT " + limit.getLimit() + offsetSql;
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
    public String visit(SqlOrderBy orderBy) {
        // ORDER BY <expr> [ASC/DESC] [NULLS FIRST/LAST]
        // ASC and NULLS LAST are default in EXASOL
        List<String> sqlOrderElement = new ArrayList<>();
        for (int i = 0; i < orderBy.getExpressions().size(); ++i) {
            String elementSql = orderBy.getExpressions().get(i).accept(this);
            boolean shallNullsBeAtTheEnd = orderBy.nullsLast().get(i);
            boolean isAscending = orderBy.isAscending().get(i);
            if (isAscending == false) {
                elementSql += " DESC";
            }
            if (shallNullsBeAtTheEnd != nullsAreAtEndByDefault(isAscending, HiveProperties.getDefaultNullSorting())) {
                // we have to specify null positioning explicitly, otherwise it would be wrong
                elementSql += (shallNullsBeAtTheEnd) ? " NULLS LAST" : " NULLS FIRST";
            }
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + Joiner.on(", ").join(sqlOrderElement);
    }

    /**
     * @param isAscending           true if the desired sort order is ascending, false if descending
     * @param defaultNullSorting    default null sorting of dialect
     * @return true, if the data source would position nulls at end of the resultset if NULLS FIRST/LAST is not specified explicitly.
     */
    private boolean nullsAreAtEndByDefault(boolean isAscending, HiveProperties.NullSorting defaultNullSorting) {
        if (defaultNullSorting == HiveProperties.NullSorting.NULLS_SORTED_AT_END) {
            return true;
        } else if (defaultNullSorting == HiveProperties.NullSorting.NULLS_SORTED_AT_START) {
            return false;
        } else {
            if (isAscending) {
                return (defaultNullSorting == HiveProperties.NullSorting.NULLS_SORTED_HIGH);
            } else {
                return !(defaultNullSorting == HiveProperties.NullSorting.NULLS_SORTED_HIGH);
            }
        }
    }

    @Override
    public String visit(SqlPredicateAnd predicate) {
        List<String> operandsSql = new ArrayList<>();
        for (SqlNode node : predicate.getAndedPredicates()) {
            operandsSql.add(node.accept(this));
        }
        return "(" + Joiner.on(" AND ").join(operandsSql) + ")";
    }

    @Override
    public String visit(SqlPredicateBetween predicate) {
        return predicate.getExpression().accept(this) + " BETWEEN "
                + predicate.getBetweenLeft().accept(this) + " AND "
                + predicate.getBetweenRight().accept(this);
    }

    @Override
    public String visit(SqlPredicateEqual predicate) {
        SqlNode leftNode = predicate.getLeft();
        SqlNode rightNode = predicate.getRight();
        if(!loadAllPartitions){
            if(leftNode.getType().equals(SqlNodeType.COLUMN) && rightNode.getType().equals(SqlNodeType.LITERAL_STRING)){
                SqlColumn sqlColumn = (SqlColumn) leftNode;
                if(partitionColumns.contains(sqlColumn.getName())){
                    selectedPartitions.add(sqlColumn.getName()+"=" + rightNode);
                    numberOfPartitionColumns++;
                }
                else{
                    loadAllPartitions = true;
                }
            }
            else if(rightNode.getType().equals(SqlNodeType.COLUMN) && leftNode.getType().equals(SqlNodeType.LITERAL_STRING)){
                SqlColumn sqlColumn = (SqlColumn) rightNode;
                if(partitionColumns.contains(sqlColumn.getName())){
                    selectedPartitions.add(sqlColumn.getName() +"=" + leftNode);
                    numberOfPartitionColumns++;
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
    public String visit(SqlPredicateInConstList predicate) {
        List<String> argumentsSql = new ArrayList<>();
        boolean isListOfStrings = true;
        for (SqlNode node : predicate.getInArguments()) {
            if(!node.getType().equals(SqlNodeType.LITERAL_STRING)){
                isListOfStrings = false;
            }

            argumentsSql.add(node.accept(this));
        }
        SqlNode expression = predicate.getExpression();
        if(isListOfStrings && !loadAllPartitions) {
            if (expression.getType().equals(SqlNodeType.COLUMN)) {
                numberOfPartitionColumns++;
                SqlColumn sqlColumn = (SqlColumn) expression;
                for(String argument:argumentsSql){
                    selectedPartitions.add(sqlColumn.getName()+ "=" +argument);
                }
            }
        }

        return expression.accept(this) + " IN ("
                + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public String visit(SqlPredicateLess predicate) {
        return predicate.getLeft().accept(this) + " < "
                + predicate.getRight().accept(this);
    }

    @Override
    public String visit(SqlPredicateLessEqual predicate) {
        return predicate.getLeft().accept(this) + " <= "
                + predicate.getRight().accept(this);
    }

    @Override
    public String visit(SqlPredicateLike predicate) {
        String sql = predicate.getLeft().accept(this) + " LIKE "
                + predicate.getPattern().accept(this);
        if (predicate.getEscapeChar() != null) {
            sql += " ESCAPE " + predicate.getEscapeChar().accept(this);
        }
        return sql;
    }

    @Override
    public String visit(SqlPredicateLikeRegexp predicate) {
        return predicate.getLeft().accept(this) + " REGEXP_LIKE "
                + predicate.getPattern().accept(this);
    }

    @Override
    public String visit(SqlPredicateNot predicate) {
        // "SELECT NOT NOT TRUE" is invalid syntax, "SELECT NOT (NOT TRUE)" works.
        return "NOT (" + predicate.getExpression().accept(this) + ")";
    }

    @Override
    public String visit(SqlPredicateNotEqual predicate) {
        return predicate.getLeft().accept(this) + " != "
                + predicate.getRight().accept(this);
    }

    @Override
    public String visit(SqlPredicateOr predicate) {
        List<String> operandsSql = new ArrayList<>();
        for (SqlNode node : predicate.getOrPredicates()) {
            operandsSql.add(node.accept(this));
        }
        return "(" + Joiner.on(" OR ").join(operandsSql) + ")";
    }

    @Override
    public String visit(SqlPredicateIsNull predicate) {
        return predicate.getExpression().accept(this) + " IS NULL";
    }

    @Override
    public String visit(SqlPredicateIsNotNull predicate) {
        return predicate.getExpression().accept(this) + " IS NOT NULL";

    }

}
