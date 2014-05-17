package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.junit.Test;

public class QueryOptimizerTest {

  @Test
  public void test() {

    Node query = new QueryBuilder().and().and().or().end().end().end().build();

    QueryOptimizer optimizer = new QueryOptimizer(query);

    System.out.println(new NodeToJexl().transform(optimizer.getOptimizedQuery()));

  }
}
