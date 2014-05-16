package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.Map;
import java.util.Set;

public interface GlobalIndexVisitor extends NodeVisitor {

  Map<CardinalityKey, Long> getCardinalities();

  Set<String> getShards();

  void exec();
}
