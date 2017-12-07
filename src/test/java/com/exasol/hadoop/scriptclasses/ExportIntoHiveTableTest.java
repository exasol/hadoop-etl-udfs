package com.exasol.hadoop.scriptclasses;

import com.exasol.ExaIterator;
import com.exasol.ExaIteratorDummy;
import com.exasol.ExaMetadata;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ExportIntoHiveTableTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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

    @Ignore("webHCAT access comes before HDFS access, so exception message is different: Problem accessing webHCAT")
    @Test
    public void testHdfsUrlListSplit() throws Exception {
        List<List<Object>> paramSet = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        params.add("dummy");
        params.add("dummy");
        params.add("dummy");
        params.add("dummy");
        params.add("hdfs://__hdfsnn01:9999,hdfs://__hdfsnn02:9999,hdfs://__hdfsnn03:9999");
        params.add("dummy");
        params.add("dummy");
        params.add("dummy");
        params.add("dummy");
        params.add("dummy");
        params.add(null);
        paramSet.add(params);
        ExaIterator iter = new ExaIteratorDummy(paramSet);
        ExaMetadata meta = mock(ExaMetadata.class);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("no host: hdfs://__hdfsnn01:9999");
        expectedException.expectMessage("no host: hdfs://__hdfsnn02:9999");
        expectedException.expectMessage("no host: hdfs://__hdfsnn03:9999");
        ExportIntoHiveTable.run(meta, iter);
    }
}
