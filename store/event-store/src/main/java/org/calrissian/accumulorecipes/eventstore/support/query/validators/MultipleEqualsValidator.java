package org.calrissian.accumulorecipes.eventstore.support.query.validators;


import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.visitor.NodeVisitor;

import java.util.HashSet;
import java.util.Set;

public class MultipleEqualsValidator implements NodeVisitor {

    @Override
    public void begin(ParentNode node) {
        if (node instanceof AndNode) {

            Set<String> keys = new HashSet<String>();
            for (Node child : node.children()) {
                if (child instanceof EqualsLeaf) {
                    EqualsLeaf leaf = (EqualsLeaf) child;

                    if (!keys.add(leaf.getKey()))
                        throw new IllegalArgumentException("Can not have equals with same key in an and clause");
                }
            }
        }
    }

    @Override
    public void end(ParentNode node) {
    }

    @Override
    public void visit(Leaf node) {
    }
}
