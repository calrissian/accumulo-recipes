package org.calrissian.accumulorecipes.commons.iterators.support;

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.types.TypeRegistry;

import static org.calrissian.accumulorecipes.commons.support.Constants.INNER_DELIM;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;

public class NodeToJexl {

  TypeRegistry<String> registry = ACCUMULO_TYPES; //TODO make types configurable

  public String transform(Node node) {
    try {
      if(node instanceof ParentNode)
        return processParent(node);
      else
        return processChild(node);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String processParent(Node node) throws Exception {

    StringBuilder builder = new StringBuilder("(");
    for(int i = 0; i < node.children().size(); i++) {

      Node child = node.children().get(i);
      String newJexl;
      if(child instanceof ParentNode)
        newJexl = processParent(child);
      else
        newJexl = processChild(child);

      builder.append(newJexl);

      if(i < node.children().size() - 1) {
        if(node instanceof AndNode)
          builder.append(" and ");
        else if(node instanceof OrNode)
          builder.append(" or ");
      }
    }

    return builder.append(")").toString();

  }

  private String processChild(Node node) throws Exception {

    StringBuilder builder = new StringBuilder("(");

    if(node instanceof EqualsLeaf) {
      EqualsLeaf leaf = (EqualsLeaf)node;
      return builder.append(leaf.getKey()).append(" == '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
            .toString();
    } else if(node instanceof NotEqualsLeaf) {
      NotEqualsLeaf leaf = (NotEqualsLeaf)node;
      return builder.append(leaf.getKey()).append(" != '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
            .toString();
    } else if(node instanceof RangeLeaf) {
      RangeLeaf leaf = (RangeLeaf)node;
      return builder.append(leaf.getKey()).append(" >= '")
              .append(registry.getAlias(leaf.getStart())).append(INNER_DELIM)
              .append(registry.encode(leaf.getStart())).append("')")
              .append(" and ('").append(leaf.getKey()).append(" <= '")
              .append(registry.getAlias(leaf.getEnd())).append(INNER_DELIM)
              .append(registry.encode(leaf.getEnd())).append("')")
            .toString();
    } else if(node instanceof GreaterThanLeaf) {
      GreaterThanLeaf leaf = (GreaterThanLeaf)node;
      return builder.append(leaf.getKey()).append(" > '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if(node instanceof GreaterThanEqualsLeaf) {
      GreaterThanEqualsLeaf leaf = (GreaterThanEqualsLeaf)node;
      return builder.append(leaf.getKey()).append(" >= '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if(node instanceof LessThanLeaf) {
      LessThanLeaf leaf = (LessThanLeaf)node;
      return builder.append(leaf.getKey()).append(" < '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if(node instanceof LessThanEqualsLeaf) {
      LessThanEqualsLeaf leaf = (LessThanEqualsLeaf)node;
      return builder.append(leaf.getKey()).append(" <= '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else {
      throw new RuntimeException("An unsupported leaf type was encountered: " + node.getClass().getName());
    }
  }
}
