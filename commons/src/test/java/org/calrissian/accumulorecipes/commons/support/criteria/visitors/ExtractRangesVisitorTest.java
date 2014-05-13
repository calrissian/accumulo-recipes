package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.EqualsLeaf;
import org.calrissian.mango.criteria.domain.GreaterThanLeaf;
import org.calrissian.mango.criteria.domain.Node;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExtractRangesVisitorTest {

  @Test
  public void testExtract_onlyRanges() {

    Node node = new QueryBuilder().and().greaterThan("key", "val").lessThan("key", "val").end().build();


    ExtractRangesVisitor rangesVisitor = new ExtractRangesVisitor();
    node.accept(rangesVisitor);

    rangesVisitor.extract();

    assertEquals(1, node.children().size());
    assertTrue(node.children().get(0) instanceof GreaterThanLeaf);
  }

  @Test
  public void testExtract_rangesAndNonRanges() {

    Node node = new QueryBuilder().and().greaterThan("key", "val").lessThan("key", "val").eq("key", "val").end().build();

    ExtractRangesVisitor rangesVisitor = new ExtractRangesVisitor();
    node.accept(rangesVisitor);

    rangesVisitor.extract();

    assertEquals(1, node.children().size());
    assertTrue(node.children().get(0) instanceof EqualsLeaf);
  }

}
