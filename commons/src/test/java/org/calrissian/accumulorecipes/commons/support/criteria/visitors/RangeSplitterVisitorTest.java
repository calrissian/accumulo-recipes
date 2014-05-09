package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeSplitterVisitorTest {

  @Test
  public void test() {

    Node query = new QueryBuilder().and().eq("key1", "val1").range("key2", 0, 5).end().build();
    query.accept(new RangeSplitterVisitor());

    assertTrue(query instanceof AndNode);
    assertEquals(3, query.children().size());
    assertTrue(query.children().get(0) instanceof EqualsLeaf);
    assertTrue(query.children().get(1) instanceof GreaterThanEqualsLeaf);
    assertTrue(query.children().get(2) instanceof LessThanEqualsLeaf);

    query = new QueryBuilder().or().eq("key1", "val1").range("key2", 0, 5).end().build();
    query.accept(new RangeSplitterVisitor());

    assertTrue(query instanceof OrNode);
    assertEquals(2, query.children().size());
    assertTrue(query.children().get(0) instanceof EqualsLeaf);
    assertTrue(query.children().get(1) instanceof AndNode);
    assertTrue(query.children().get(1).children().get(0) instanceof GreaterThanEqualsLeaf);
    assertTrue(query.children().get(1).children().get(1) instanceof LessThanEqualsLeaf);

  }

}
