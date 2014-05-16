package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import com.sun.org.apache.xpath.internal.operations.Neg;
import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeEncodingException;

import java.util.*;

import static java.util.Collections.sort;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class CardinalityReorderVisitor implements NodeVisitor {

  private static TypeRegistry<String> registry = LEXI_TYPES;
  private Map<CardinalityKey, Long> cardinalities;
  private Map<String, Set<CardinalityKey>> keyToCarinalityKey = new HashMap<String, Set<CardinalityKey>>();

  public CardinalityReorderVisitor(Map<CardinalityKey, Long> cardinalities) {
    this.cardinalities = cardinalities;
    for (CardinalityKey key : cardinalities.keySet()) {
      Set<CardinalityKey> cardinalityKey = keyToCarinalityKey.get(key.getKey());
      if (cardinalityKey == null) {
        cardinalityKey = new HashSet<CardinalityKey>();
        keyToCarinalityKey.put(key.getKey(), cardinalityKey);
      }
      cardinalityKey.add(key);
    }
  }

  @Override
  public void begin(ParentNode parentNode) {
    List<CardinalityNode> newCardinalities = new ArrayList<CardinalityNode>();

    long totalCardinality = 0;
    for (Node child : parentNode.children()) {
      CardinalityNode cardinalityNode = null;
      if (child instanceof AndNode || child instanceof OrNode)
        cardinalityNode = new CardinalityNode(getCardinality((ParentNode) child), child);
      else if (child instanceof Leaf)
        cardinalityNode = new CardinalityNode(getCardinality((Leaf) child), child);
      else
        throw new RuntimeException("Unexpected node encountered while calculating cardinalities: " + child.getClass());

      if (parentNode instanceof AndNode && cardinalityNode.cardinality == 0) {
        if(parentNode.parent() != null)
          parentNode.parent().removeChild(parentNode);
        else
          parentNode.children().clear();
        break;
      } else {
        newCardinalities.add(cardinalityNode);
        totalCardinality += cardinalityNode.cardinality;
      }
    }

    if(parentNode instanceof OrNode && totalCardinality == 0) {
      if(parentNode.parent() != null)
        parentNode.parent().removeChild(parentNode);
      else
        parentNode.children().clear();
    } else {
      sort(newCardinalities);
      parentNode.children().clear();
      for (CardinalityNode cnode : newCardinalities)
        parentNode.addChild(cnode.getNode());
    }
  }

  private long getCardinality(ParentNode node) {

    long cardinality = 0;
    for (Node child : node.children()) {
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

    AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

    // hasKey and hasNotKey need special treatment since we don't know the aliases
    if (leaf instanceof HasLeaf || leaf instanceof HasNotLeaf) {
      Set<CardinalityKey> cardinalityKeys = keyToCarinalityKey.get(kvLeaf.getKey());

      Long cardinality = 0l;
      for (CardinalityKey key : cardinalityKeys) {
        cardinality += this.cardinalities.get(key);
      }

      return cardinality;
    } else {
      String alias = registry.getAlias(kvLeaf.getValue());
      String normalizedVal = null;
      try {
        normalizedVal = registry.encode(kvLeaf.getValue());
      } catch (TypeEncodingException e) {
        throw new RuntimeException(e);
      }

      CardinalityKey cardinalityKey = new BaseCardinalityKey(kvLeaf.getKey(), normalizedVal, alias);
      Long cardinality = cardinalities.get(cardinalityKey);

      if(cardinality == null) {
        if(!(leaf instanceof NegationLeaf))
          return 0;
        else if(leaf instanceof NegationLeaf)
          return 1; // need to fake the cardinality here in order for it to stay in the tree.
      }

      return cardinality != null ? cardinality : 0;
    }
  }

  @Override
  public void end(ParentNode parentNode) {

  }

  @Override
  public void visit(Leaf leaf) {

  }

  private class CardinalityNode implements Comparable<CardinalityNode> {
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
