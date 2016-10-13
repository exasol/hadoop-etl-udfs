package com.exasol.jsonpath;

import com.exasol.hadoop.hcat.HCatTableColumn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OutputColumnSpecificationTest {
    
    @Test
    public void testParseOutputSpecification() {
        // Without JsonPath
        List<HCatTableColumn> columns = Arrays.asList(new HCatTableColumn("c1", "dummy"), new HCatTableColumn("c2", "dummy"), new HCatTableColumn("c3", "dummy"), new HCatTableColumn("c4", "dummy"));
        List<OutputColumnSpec> expected = new ArrayList<>();
        expected.add(new OutputColumnSpec(0, "c1", 0));
        expected.add(new OutputColumnSpec(1, "c2", 1));
        expected.add(new OutputColumnSpec(2, "c3", 2));
        expected.add(new OutputColumnSpec(3, "c4", 3));
        String spec = "c1, c2 , c3,c4";
        List<OutputColumnSpec> actual = OutputColumnSpecUtil.parseOutputSpecification(spec, columns);
        assertEquals(expected, actual);
        
        // with JsonPath specification
        expected.clear();
        expected.add(new OutputColumnSpec(0, "c1", 0, Arrays.asList((JsonPathElement)new JsonPathFieldElement("f1"))));
        expected.add(new OutputColumnSpec(1, "c2", 1, Arrays.asList((JsonPathElement)new JsonPathFieldElement("f1"), new JsonPathListIndexElement(0), new JsonPathFieldElement("f2"))));
        expected.add(new OutputColumnSpec(2, "c3", 2, Arrays.asList((JsonPathElement)new JsonPathListIndexElement(1))));
        expected.add(new OutputColumnSpec(3, "c4", 3));
        spec = "c1.f1, c2.f1[0].f2, c3[1], c4";
        actual = OutputColumnSpecUtil.parseOutputSpecification(spec, columns);
        assertEquals(expected, actual);

        // With partitions, and mixed up column positions
        columns = Arrays.asList(new HCatTableColumn("c1", "dummy"), new HCatTableColumn("c2", "dummy"), new HCatTableColumn("c3", "dummy"));
        List<HCatTableColumn> partitions = Arrays.asList(new HCatTableColumn("p1", "dummy"));
        expected.clear();
        expected.add(new OutputColumnSpec(0, "c2", 1));
        expected.add(new OutputColumnSpec(1, "c1", 0));
        expected.add(new OutputColumnSpec(2, "c2", 1));
        expected.add(new OutputColumnSpec(3, "p1", 3));
        expected.add(new OutputColumnSpec(4, "c3", 2));
        spec = "c2, c1, c2, p1, c3";
        actual = OutputColumnSpecUtil.parseOutputSpecification(spec, columns, partitions);
        assertEquals(expected, actual);
    }

}
