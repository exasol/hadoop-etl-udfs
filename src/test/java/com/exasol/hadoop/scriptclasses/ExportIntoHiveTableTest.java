package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaIterator;
import com.exasol.ExaIteratorDummy;
import com.exasol.ExaMetadata;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ExportIntoHiveTableTest {

	@Test(expected = RuntimeException.class)
	public void testOptionalParams() throws Exception {
		List<List<Object>> paramSet = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		params.add("dummy");
		params.add("dummy");
		params.add("dummy");
		params.add("dummy");
		params.add("dummy");
		params.add("dummy");
		params.add("dummy");
		params.add("dummy");
		params.add(null); // auth connection name at position PARAM_IDX_AUTH_CONNECTION
		params.add("dummy");
		params.add(null); // debug address at position PARAM_IDX_DEBUG_ADDRESS
		paramSet.add(params);
		ExaIterator iter = new ExaIteratorDummy(paramSet);
		ExaMetadata meta = mock(ExaMetadata.class);

		try {
			ExportIntoHiveTable.run(meta, iter);
		} catch (NullPointerException npe) {
			fail("A RuntimeException was expected, but not a NullPointerException!");
		}
	}
}
