package com.exasol.jsonpath;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class JsonPathTest {

	@Test
	public void testParseJsonPath() {
		List<JsonPathElement> expectedPath = new ArrayList<>();
		expectedPath.add(new JsonPathFieldElement("f1"));
		expectedPath.add(new JsonPathFieldElement("f2"));
		expectedPath.add(new JsonPathListIndexElement(0));
		expectedPath.add(new JsonPathFieldElement("f3.x"));
		expectedPath.add(new JsonPathFieldElement("f4"));
		expectedPath.add(new JsonPathListIndexElement(1));
		expectedPath.add(new JsonPathListIndexElement(2));
		
		String pathSpec = "f1.f2[0].\"f3.x\".f4[1][2]";
		
		List<JsonPathElement> actualPath = JsonPathParser.parseJsonPath(pathSpec);

		assertEquals(expectedPath, actualPath);
	}
	
}
