package org.calrissian.accumulorecipes.eventstore.support;

import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.junit.Test;

public class NodeToJexlTest {

  private NodeToJexl nodeToJexl = new NodeToJexl();

  @Test
  public void testSimpleEquals() {

    System.out.println(nodeToJexl.transform(new QueryBuilder().eq("key1", true).build()));

  }
}
