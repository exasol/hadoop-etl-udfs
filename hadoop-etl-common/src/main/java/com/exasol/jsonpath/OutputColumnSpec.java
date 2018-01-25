package com.exasol.jsonpath;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class OutputColumnSpec {
	int resultPosition;            // the position in the resultset for this column (starting with 0)
	String columnName;
	int columnPosition;            // the position of the column in the original table (starting with 0). Might point to a partition column.
	List<JsonPathElement> path;
	
	public OutputColumnSpec(int resultPosition, String columnName, int columnPosition) {
		this.resultPosition = resultPosition;
		this.columnName = columnName;
		this.columnPosition = columnPosition;
		this.path = new ArrayList<>();
	}
	
	public OutputColumnSpec(int resultPosition, String columnName, int columnPosition, List<JsonPathElement> path) {
		this.resultPosition = resultPosition;
		this.columnName = columnName;
		this.columnPosition = columnPosition;
		this.path = path;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public int getColumnPosition() {
		return columnPosition;
	}
	
	public List<JsonPathElement> getPath() {
		return path;
	}
	
	public int getResultPosition() {
		return resultPosition;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (!(obj instanceof OutputColumnSpec)) {
    	    return false;
        }
        if (obj == this) {
    	    return true;
        }
		OutputColumnSpec other = (OutputColumnSpec) obj;
		return other.getColumnName().equals(columnName)
				&& other.getColumnPosition() == columnPosition
				&& other.getPath().equals(path)
				&& other.getResultPosition() == resultPosition;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(17,31).append(resultPosition)
				.append(columnName)
				.append(columnPosition)
				.append(path)
				.toHashCode();
	}
	
	@Override
	public String toString() {
		return "OutputColumnSpec[" + resultPosition + ":" + columnName + ":" + columnPosition + ":" + path + "]";
	}
}
