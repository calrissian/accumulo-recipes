package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import static java.util.Arrays.asList;

public class RangeSplitterVisitor implements NodeVisitor {

  @Override
  public void begin(ParentNode parentNode) {}

  @Override
  public void end(ParentNode parentNode) {}

  @Override
  public void visit(Leaf leaf) {

    if(leaf instanceof RangeLeaf) {
      GreaterThanEqualsLeaf lhs =
              new GreaterThanEqualsLeaf(((RangeLeaf) leaf).getKey(), ((RangeLeaf) leaf).getStart(), leaf.parent());
      LessThanEqualsLeaf rhs =
              new LessThanEqualsLeaf(((RangeLeaf) leaf).getKey(), ((RangeLeaf) leaf).getEnd(), leaf.parent());

      leaf.parent().removeChild(leaf);
      if(leaf.parent() instanceof  AndNode) {
        leaf.parent().addChild(lhs);
        leaf.parent().addChild(rhs);
      } else {
        AndNode node = new AndNode(leaf.parent(), asList(new Node[]{lhs, rhs}));
        leaf.parent().addChild(node);
      }
    }
  }
}
