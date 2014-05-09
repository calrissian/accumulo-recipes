package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.HashSet;
import java.util.Set;

public class QueryKeysExtractorVisitor implements NodeVisitor {

  private Set<String> keysFound = new HashSet<String>();

  @Override
  public void begin(ParentNode parentNode) {

  }

  @Override
  public void end(ParentNode parentNode) {

  }

  @Override
  public void visit(Leaf leaf) {
    keysFound.add(((AbstractKeyValueLeaf)leaf).getKey());
  }

  public Set<String> getKeysFound() {
    return keysFound;
  }
}
