/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.iterators.support;

import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.types.TypeRegistry;

import static org.calrissian.accumulorecipes.commons.support.Constants.INNER_DELIM;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class NodeToJexl {

  TypeRegistry<String> registry = LEXI_TYPES; //TODO make types configurable

  public String transform(Node node) {
    try {
      if (node instanceof ParentNode)
        return processParent(node);
      else
        return processChild(node);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String processParent(Node node) throws Exception {

    StringBuilder builder = new StringBuilder("(");
    for (int i = 0; i < node.children().size(); i++) {

      Node child = node.children().get(i);
      String newJexl;
      if (child instanceof ParentNode)
        newJexl = processParent(child);
      else
        newJexl = processChild(child);

      builder.append(newJexl);

      if (i < node.children().size() - 1) {
        if (node instanceof AndNode)
          builder.append(" and ");
        else if (node instanceof OrNode)
          builder.append(" or ");
      }
    }

    return builder.append(")").toString();

  }

  private String processChild(Node node) throws Exception {

    StringBuilder builder = new StringBuilder();

    if (node instanceof EqualsLeaf) {
      builder.append("(");
      EqualsLeaf leaf = (EqualsLeaf) node;
      return builder.append(leaf.getKey()).append(" == '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if (node instanceof NotEqualsLeaf) {
      builder.append("(");
      NotEqualsLeaf leaf = (NotEqualsLeaf) node;
      return builder.append(leaf.getKey()).append(" != '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if (node instanceof RangeLeaf) {
      builder.append("(");
      RangeLeaf leaf = (RangeLeaf) node;
      return builder.append(leaf.getKey()).append(" >= '")
              .append(registry.getAlias(leaf.getStart())).append(INNER_DELIM)
              .append(registry.encode(leaf.getStart())).append("')")
              .append(" and ('").append(leaf.getKey()).append(" <= '")
              .append(registry.getAlias(leaf.getEnd())).append(INNER_DELIM)
              .append(registry.encode(leaf.getEnd())).append("')")
              .toString();
    } else if (node instanceof GreaterThanLeaf) {
      builder.append("(");
      GreaterThanLeaf leaf = (GreaterThanLeaf) node;
      return builder.append(leaf.getKey()).append(" > '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if (node instanceof GreaterThanEqualsLeaf) {
      builder.append("(");
      GreaterThanEqualsLeaf leaf = (GreaterThanEqualsLeaf) node;
      return builder.append(leaf.getKey()).append(" >= '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if (node instanceof LessThanLeaf) {
      builder.append("(");
      LessThanLeaf leaf = (LessThanLeaf) node;
      return builder.append(leaf.getKey()).append(" < '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if (node instanceof LessThanEqualsLeaf) {
      builder.append("(");
      LessThanEqualsLeaf leaf = (LessThanEqualsLeaf) node;
      return builder.append(leaf.getKey()).append(" <= '")
              .append(registry.getAlias(leaf.getValue())).append(INNER_DELIM)
              .append(registry.encode(leaf.getValue())).append("')")
              .toString();
    } else if (node instanceof HasLeaf) {
      builder.append("(");
      HasLeaf leaf = (HasLeaf) node;
      return builder.append(leaf.getKey()).append(" >= '\u0000')")
              .toString();
    } else if (node instanceof HasNotLeaf) {
      builder.append("!(");
      HasNotLeaf leaf = (HasNotLeaf) node;
      return builder.append(leaf.getKey()).append(" >= '\u0000')")
              .toString();
    } else {
      throw new RuntimeException("An unsupported leaf type was encountered: " + node.getClass().getName());
    }
  }
}
