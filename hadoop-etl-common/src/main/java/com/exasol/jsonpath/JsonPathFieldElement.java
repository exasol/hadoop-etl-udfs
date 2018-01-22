package com.exasol.jsonpath;


public class JsonPathFieldElement implements JsonPathElement {
	
	private String fieldName;
	
	public JsonPathFieldElement(String fieldName) {
		this.fieldName = fieldName;
	}
	
	public String getFieldName() {
		return fieldName;
	}

	@Override
	public Type getType() {
		return Type.FIELD;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (!(obj instanceof JsonPathFieldElement)) {
    	    return false;
        }
        if (obj == this) {
    	    return true;
        }
        return ((JsonPathFieldElement)obj).getFieldName().equals(fieldName);
	}
	
	@Override
	public int hashCode() {
		return fieldName.hashCode();
	}
	
	@Override
	public String toString() {
	    return this.getClass().getName() + "[" + fieldName + "]";
	}

}
