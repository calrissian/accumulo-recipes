/*
 * Copyright (C) 2015 The Calrissian Authors
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
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

public class CalculateShardsVisitor implements NodeVisitor {

    private final Map<CardinalityKey,Set<String>> keysToShards;
    private Map<String, Set<CardinalityKey>> keyToCarinalityKey = new HashMap<String, Set<CardinalityKey>>();
    private TypeRegistry<String> registry;

    private Set<String> finalShards;

    public CalculateShardsVisitor(Map<CardinalityKey,Set<String>> shards, TypeRegistry<String> registry) {
        this.keysToShards = shards;
        this.registry = registry;

        // TODO: This is shared w/ the ReorderVisitor- pull it out into a central location
        for (CardinalityKey key : shards.keySet()) {
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
        if(finalShards == null)
            finalShards = getShards(parentNode);
    }

    @Override
    public void end(ParentNode parentNode) {

    }

    @Override
    public void visit(Leaf leaf) {

    }

    public Set<String> getShards() {
        return finalShards;
    }

    private Set<String> getShards(ParentNode node) {

        Set<Set<String>> finalShards = new HashSet<Set<String>>();
        for (Node child : node.children()) {
            if (child instanceof AndNode || child instanceof OrNode)
                finalShards.add(getShards((ParentNode) child));
            else if (child instanceof Leaf)  {
                Set<String> childShards = getShards((Leaf) child);


                //if a negated leaf didn't return any shards, we should just ignore it
                finalShards.add(childShards);
            }
        }

        Set<String> combinedSet = null;
        // if the parent is an AndNode, we need to intersect keysToShards
        if(node instanceof AndNode) {
            for(Set<String> curShards : finalShards) {
                if(combinedSet == null)
                    combinedSet = curShards;
                else {
                    Set<String> intersected = Sets.intersection(combinedSet, curShards);
                    combinedSet = new HashSet<String>(intersected);
                }
            }

        // otherwise the paren is an OrNode, and we need to union
        } else {
            for(Set<String> curShards : finalShards) {
                if(combinedSet == null)
                    combinedSet = Sets.newHashSet();
                combinedSet.addAll(curShards);
            }
        }

        return combinedSet;
    }

    private Set<String> getShards(Leaf leaf) {
        AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

        // hasKey and hasNotKey need special treatment since we don't know the aliases
        if (leaf instanceof HasLeaf || leaf instanceof HasNotLeaf || NodeUtils.isRangeLeaf(leaf)) {
            Set<CardinalityKey> cardinalityKeys = keyToCarinalityKey.get(kvLeaf.getKey());
            Set<String> unionedShards = new HashSet<String>();
            if (cardinalityKeys == null) {
                if (leaf instanceof NegationLeaf)
                    return unionedShards;
            } else {
                for (CardinalityKey key : cardinalityKeys) {
                    unionedShards.addAll(this.keysToShards.get(key));
                }
            }

            return unionedShards;
        } else {
            String alias = registry.getAlias(kvLeaf.getValue());
            String normalizedVal = null;
            normalizedVal = registry.encode(kvLeaf.getValue());

            CardinalityKey cardinalityKey = new BaseCardinalityKey(kvLeaf.getKey(), normalizedVal, alias);
            Set<String> leafShards = keysToShards.get(cardinalityKey);

            if (leafShards == null)
                return Sets.newHashSet();

            return leafShards;
        }
    }
}
