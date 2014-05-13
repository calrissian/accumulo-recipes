package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ExtractRangesVisitor implements NodeVisitor{

  private boolean nonRangeFound = false;
  private List<Leaf> rangeNodes = new LinkedList<Leaf>();

  @Override
  public void begin(ParentNode parentNode) {
  }

  @Override
  public void end(ParentNode parentNode) {

  }

  public void extract() {

    if(rangeNodes.size() > 0) {
      Iterator<Leaf> nodeIter = rangeNodes.iterator();
      if(!nonRangeFound)
        nodeIter.next();
      while(nodeIter.hasNext()) {
        Leaf leaf = nodeIter.next();
        leaf.parent().removeChild(leaf);
      }
    }
  }

  @Override
  public void visit(Leaf leaf) {
    if(NodeUtils.isRangeLeaf(leaf))
      rangeNodes.add(leaf);
    else
      nonRangeFound = true;
  }
}
