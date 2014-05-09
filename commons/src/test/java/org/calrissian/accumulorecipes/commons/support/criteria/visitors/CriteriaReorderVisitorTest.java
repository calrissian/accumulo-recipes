package org.calrissian.accumulorecipes.commons.support.criteria.visitors;


import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.OrNode;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CriteriaReorderVisitorTest {

  @Test
  public void test() {

    Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();
    cardinalities.put(new BaseCardinalityKey("key1", "val1", "string"), 500l);
    cardinalities.put(new BaseCardinalityKey("key2", "val2", "string"), 0l);
    cardinalities.put(new BaseCardinalityKey("key3", "val3", "string"), 1000l);

    Node node = new QueryBuilder().or().eq("key3", "val3").and().eq("key2", "val2").eq("key1", "val1")
            .end().end().build();

    node.accept(new CardinalityReorderVisitor(cardinalities));

    System.out.println(new NodeToJexl().transform(node));

    assertTrue(node instanceof OrNode);
    assertTrue(node.children().get(0) instanceof AndNode);
    assertEquals("key2", ((AbstractKeyValueLeaf)node.children().get(0).children().get(0)).getKey());
    assertEquals("key1", ((AbstractKeyValueLeaf)node.children().get(0).children().get(1)).getKey());
    assertEquals("key3", ((AbstractKeyValueLeaf)node.children().get(1)).getKey());
  }
}
