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
package org.calrissian.accumulorecipes.commons.support.criteria.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.calrissian.accumulorecipes.commons.support.criteria.BaseCardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.HasLeaf;
import org.calrissian.mango.criteria.domain.HasNotLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.NegationLeaf;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.OrNode;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.types.TypeRegistry;

import static java.util.Collections.sort;

public class CardinalityReorderVisitor implements NodeVisitor {

    private static TypeRegistry<String> registry;
    private Map<CardinalityKey, Long> cardinalities;
    private Map<String, Set<CardinalityKey>> keyToCarinalityKey = new HashMap<String, Set<CardinalityKey>>();

    public CardinalityReorderVisitor(Map<CardinalityKey, Long> cardinalities, TypeRegistry<String> typeRegistry) {
        this.cardinalities = cardinalities;
        this.registry = typeRegistry;
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

        AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

        // hasKey and hasNotKey need special treatment since we don't know the aliases
        if (leaf instanceof HasLeaf || leaf instanceof HasNotLeaf || NodeUtils.isRangeLeaf(leaf)) {
            Set<CardinalityKey> cardinalityKeys = keyToCarinalityKey.get(kvLeaf.getKey());

            Long cardinality = 0l;
            if (cardinalityKeys == null) {
                if (leaf instanceof NegationLeaf)
                    return 1;
            } else {
                for (CardinalityKey key : cardinalityKeys) {
                    cardinality += this.cardinalities.get(key);
                }
            }

            return cardinality;
        } else {
            String alias = registry.getAlias(kvLeaf.getValue());
            String normalizedVal = null;
            normalizedVal = registry.encode(kvLeaf.getValue());

            CardinalityKey cardinalityKey = new BaseCardinalityKey(kvLeaf.getKey(), normalizedVal, alias);
            Long cardinality = cardinalities.get(cardinalityKey);

            if (cardinality == null) {
                if (leaf instanceof NegationLeaf)
                    return 1;
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
