package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractInNotInVisitorTest {

    @Test
    public void testIn() {

        Node query = new QueryBuilder().in("key", "hello", "goodbye").build();
        Node expected = new QueryBuilder().and().or().eq("key", "hello").eq("key", "goodbye").end().end().build();
        query.accept(new ExtractInNotInVisitor());

        assertEquals(expected, query);
    }
}
