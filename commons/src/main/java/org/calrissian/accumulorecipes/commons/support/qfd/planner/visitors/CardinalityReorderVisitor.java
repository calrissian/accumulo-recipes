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
package org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors;

import org.calrissian.accumulorecipes.commons.support.qfd.AttributeIndexKey;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.types.TypeRegistry;

import java.util.*;

import static java.util.Collections.sort;

/**
 * This visitor is an optimization that looks up cardinality information in the index table and reorders
 * the leaves and subtrees of the queries to minimize the amount of items that need to be queried. A good
 * example is in the case of a foreach. If I am looking for entries with x = 1 and y = 2 and the cardinalities
 * are as follows: {x: 5M, y: 5}, we would benefit greatly from looking throw all the y's first and finding
 * all entries with a y = 2 that also have an x = 1.
 */
public class CardinalityReorderVisitor implements NodeVisitor {

    private TypeRegistry<String> registry;
    private Map<AttributeIndexKey, Long> cardinalities;
    private Map<String, Set<AttributeIndexKey>> accumuloKeyToAttributeIndexKey = new HashMap<String, Set<AttributeIndexKey>>();

    public CardinalityReorderVisitor(Map<AttributeIndexKey, Long> cardinalities, TypeRegistry<String> typeRegistry) {
        this.cardinalities = cardinalities;
        this.registry = typeRegistry;
        for (AttributeIndexKey key : cardinalities.keySet()) {
            if (!accumuloKeyToAttributeIndexKey.containsKey(key.getKey())) {
                accumuloKeyToAttributeIndexKey.put(key.getKey(), com.google.common.collect.Sets.<AttributeIndexKey>newHashSet());
            }
            accumuloKeyToAttributeIndexKey.get(key.getKey()).add(key);
        }
    }

    @Override
    public void begin(ParentNode parentNode) {
        List<CardinalityNode> newCardinalities = new ArrayList<CardinalityNode>();

        long totalCardinality = 0;
        for (Node child : parentNode.children()) {
            CardinalityNode cardinalityNode;
            if (child instanceof AndNode || child instanceof OrNode)
                cardinalityNode = new CardinalityNode(getCardinality((ParentNode) child), child);
            else if (child instanceof Leaf)
                cardinalityNode = new CardinalityNode(getCardinality((Leaf) child), child);
            else
                throw new RuntimeException("Unexpected node encountered while calculating cardinalities: " + child.getClass());

            if (parentNode instanceof AndNode && cardinalityNode.cardinality == 0) {
                if (parentNode.parent() != null)
                    parentNode.parent().removeChild(parentNode);
                else
                    parentNode.children().clear();
                break;
            } else {
                newCardinalities.add(cardinalityNode);
                totalCardinality += cardinalityNode.cardinality;
            }
        }

        if (parentNode instanceof OrNode && totalCardinality == 0) {
            if (parentNode.parent() != null)
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
        /**
         * If the leaf represents an unbounded range (including !=) then it will be harder for us to zero
         * in on an exact set of shards. At the very least, we can find shards that contain the keys we're after.
         */
        // hasKey and hasNotKey need special treatment since we don't know the aliases
        if (leaf instanceof HasLeaf || leaf instanceof HasNotLeaf || NodeUtils.isRangeLeaf(leaf) || leaf instanceof NotEqualsLeaf) {
            TermLeaf termLeaf = (TermLeaf) leaf;
            Set<AttributeIndexKey> cardinalityKeys = accumuloKeyToAttributeIndexKey.get(termLeaf.getTerm());

            Long cardinality = 0L;
            if (cardinalityKeys == null) {
                if (leaf instanceof NegationLeaf)
                    return 1;
            } else {
                for (AttributeIndexKey key : cardinalityKeys) {
                    cardinality += this.cardinalities.get(key);
                }
            }

            return cardinality;

        /**
         * Just about the most efficient type of query one can do is one in which the value portion of the index can
         * be used. This will amount to a smaller more granular set of shards.
         */
        } else if (leaf instanceof TermValueLeaf) {
            TermValueLeaf termValLeaf = (TermValueLeaf) leaf;
            String alias = registry.getAlias(termValLeaf.getValue());
            String normalizedVal = null;
            normalizedVal = registry.encode(termValLeaf.getValue());

            AttributeIndexKey cardinalityKey = new AttributeIndexKey(termValLeaf.getTerm(), normalizedVal, alias);
            Long cardinality = cardinalities.get(cardinalityKey);

            if (cardinality == null) {
                if (leaf instanceof NegationLeaf)
                    return 1;
            }

            return cardinality != null ? cardinality : 0;
        }

        return 0L;
    }

    @Override
    public void end(ParentNode parentNode) {
    }

    @Override
    public void visit(Leaf leaf) {
    }

    /**
     * A private class to represent a query node with a cardinality that
     * can be sorted by the cardinality
     */
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
