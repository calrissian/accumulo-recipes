package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.sort;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;

public class CardinalityReorderVisitor implements NodeVisitor{

  private static TypeRegistry<String> registry = ACCUMULO_TYPES;
  private Map<CardinalityKey, Long> cardinalities;

  public CardinalityReorderVisitor(Map<CardinalityKey, Long> cardinalities) {
    this.cardinalities = cardinalities;
  }

  @Override
  public void begin(ParentNode parentNode) {
    List<CardinalityNode> newCardinalities = new ArrayList<CardinalityNode>();
    for(Node child : parentNode.children()) {
      if (child instanceof AndNode || child instanceof OrNode)
       newCardinalities.add(new CardinalityNode(getCardinality((ParentNode) child), child));
      else if (child instanceof Leaf)
        newCardinalities.add(new CardinalityNode(getCardinality((Leaf) child), child));
      else
        throw new RuntimeException("Unexpected node encountered while calculating cardinalities: " + child.getClass());
    }

    sort(newCardinalities);
    parentNode.children().clear();
    for(CardinalityNode cnode : newCardinalities)
      parentNode.addChild(cnode.getNode());
  }

  private long getCardinality(ParentNode node) {

    long cardinality = 0;
    for(Node child : node.children()) {
      if (child instanceof AndNode || child instanceof OrNode)
        cardinality += getCardinality((ParentNode) child);
      else if (child instanceof Leaf)
        cardinality += getCardinality((Leaf) child);
      else
        throw new RuntimeException("Unexpected node encountered while calculating cardinalities: " + child.getClass());
    }
    return cardinality;
  }

  private long getCardinality(Leaf leaf) {

    AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf)leaf;
    String alias = registry.getAlias(kvLeaf.getValue());
    String normalizedVal = null;
    try {
      normalizedVal = registry.encode(kvLeaf.getValue());
    } catch (TypeEncodingException e) {
      throw new RuntimeException(e);
    }

    CardinalityKey cardinalityKey = new BaseCardinalityKey(kvLeaf.getKey(), normalizedVal, alias);
    Long cardinality = cardinalities.get(cardinalityKey);
    return cardinality == null ? 0 : cardinality;
  }

  @Override
  public void end(ParentNode parentNode) {

  }

  @Override
  public void visit(Leaf leaf) {

  }

  private class CardinalityNode implements Comparable<CardinalityNode>{
    private Long cardinality;
    private Node node;

    private CardinalityNode(Long cardinality, Node node) {
      this.cardinality = cardinality;
      this.node = node;
    }

    public Node getNode() {
      return node;
    }

    @Override
    public int compareTo(CardinalityNode o) {
      return cardinality.compareTo(o.cardinality);
    }
  }
}
