package com.exasol.jsonpath;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonPathParser {

	public static List<JsonPathElement> parseJsonPath(String jsonPathSpecification) {
		List<JsonPathElement> path = new ArrayList<>();
		
		// Three capture groups:
		// 1) quoted field names "field.name"
		// 2) list indices in square brackets [i]
		// 3) unquoted field names (may not contain . or [)
		String regex = "\"([^\"]*)\"|\\[(\\d+)\\]|([^\\.\\[]+)";
		Matcher matcher = Pattern.compile(regex).matcher(jsonPathSpecification);
		while (matcher.find()) {
			if (matcher.group(1) != null) {
				path.add(new JsonPathFieldElement(matcher.group(1)));
			} else if (matcher.group(2) != null) {
				path.add(new JsonPathListIndexElement(Long.parseLong(matcher.group(2))));
			} else {
				assert(matcher.group(3) != null);
				path.add(new JsonPathFieldElement(matcher.group(3)));
			}
		}
		return path;
	}
}
