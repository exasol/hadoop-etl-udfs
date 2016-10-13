package com.exasol.jsonpath;

public interface JsonPathElement {

	enum Type {
		FIELD,
		LIST_INDEX
	}
	
	Type getType();
}
