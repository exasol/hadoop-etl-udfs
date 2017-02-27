package com.exasol.adapter;

import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

import java.util.*;

/**
 * Created by np on 2/7/2017.
 */
public class HiveProperties {

    protected static Set<ScalarFunction> omitParenthesesMap = new HashSet<>();

    enum NullSorting {
        // NULL values are sorted at the end regardless of sort order
        NULLS_SORTED_AT_END,

        // NULL values are sorted at the start regardless of sort order
        NULLS_SORTED_AT_START,

        // NULL values are sorted high
        NULLS_SORTED_HIGH,

        // NULL values are sorted low
        NULLS_SORTED_LOW
    }

    public static Capabilities getCapabilities() {

    Capabilities cap = new Capabilities();

        cap.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        cap.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);

        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);

        cap.supportPredicate(PredicateCapability.EQUAL);
        cap.supportPredicate(PredicateCapability.BETWEEN);
        cap.supportPredicate(PredicateCapability.IN_CONSTLIST);
        cap.supportPredicate(PredicateCapability.IS_NULL);
        cap.supportPredicate(PredicateCapability.IS_NOT_NULL);

        return cap;
    }

    public static Map<ScalarFunction, String> getScalarFunctionAliases() {
        return new EnumMap<>(ScalarFunction.class);
    }

    public static Map<AggregateFunction, String> getAggregateFunctionAliases() {
        Map<AggregateFunction, String> aliases = new HashMap<>();
        aliases.put(AggregateFunction.GEO_INTERSECTION_AGGREGATE, "ST_INTERSECTION");
        aliases.put(AggregateFunction.GEO_UNION_AGGREGATE, "ST_UNION");
        return aliases;
    }

    public static Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
        Map<ScalarFunction, String> aliases = new HashMap<>();
        aliases.put(ScalarFunction.ADD, "+");
        aliases.put(ScalarFunction.SUB, "-");
        aliases.put(ScalarFunction.MULT, "*");
        aliases.put(ScalarFunction.FLOAT_DIV, "/");
        return aliases;
    }

    public static Map<ScalarFunction, String> getPrefixFunctionAliases() {
        Map<ScalarFunction, String> aliases = new HashMap<>();
        aliases.put(ScalarFunction.NEG, "-");
        return aliases;
    }

    public static boolean omitParentheses(ScalarFunction function) {
        return omitParenthesesMap.contains(function);
    }

    public static String getStringLiteral(String value) {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }

    public static NullSorting getDefaultNullSorting() {
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SortBy
        // In Hive 2.1.0 and later, specifying the null sorting order for each of
        // the columns in the "order by" clause is supported. The default null sorting
        // order for ASC order is NULLS FIRST, while the default null sorting order for
        // DESC order is NULLS LAST.
        return NullSorting.NULLS_SORTED_LOW;
    }
}
