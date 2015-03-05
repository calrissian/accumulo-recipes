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
package org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.calrissian.accumulorecipes.commons.support.qfd.AttributeIndexKey;
import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.AndNode;
import org.calrissian.mango.criteria.domain.HasLeaf;
import org.calrissian.mango.criteria.domain.HasNotLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.NegationLeaf;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.criteria.domain.OrNode;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.criteria.visitor.NodeVisitor;
import org.calrissian.mango.types.TypeRegistry;

/**
 * An optimization function that calculates the sets of shards for each section of a given query
 * tree and tries to eliminate as many shards as possible by determining shards that will
 * never match the given query. This is done by taking intersections of the shards in AND
 * trees and taking unions of shards in OR trees.
 */
public class CalculateShardsVisitor implements NodeVisitor {

    private final Map<AttributeIndexKey,Set<String>> keysToShards;
    private Map<String, Set<AttributeIndexKey>> accumuloKeyToAttributeIndexKey = new HashMap<String, Set<AttributeIndexKey>>();
    private TypeRegistry<String> registry;

    private Set<String> finalShards;

    public CalculateShardsVisitor(Map<AttributeIndexKey,Set<String>> shards, TypeRegistry<String> registry) {
        this.keysToShards = shards;
        this.registry = registry;

        for (AttributeIndexKey key : shards.keySet()) {
            Set<AttributeIndexKey> tupleIndexKey = accumuloKeyToAttributeIndexKey.get(key.getKey());
            if (tupleIndexKey == null) {
                tupleIndexKey = new HashSet<AttributeIndexKey>();
                accumuloKeyToAttributeIndexKey.put(key.getKey(), tupleIndexKey);
            }
            tupleIndexKey.add(key);
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

        /**
         * First, we grab all shards for all subtrees (until we reach the leaves).
         */
        Set<Set<String>> resultShards = new HashSet<Set<String>>();
        for (Node child : node.children()) {
            if (child instanceof AndNode || child instanceof OrNode)
                resultShards.add(getShards((ParentNode) child));
            else if (child instanceof Leaf)  {
                Set<String> childShards = getShards((Leaf) child);
                resultShards.add(childShards);
            }
        }

        Set<String> combinedSet = null;

        /**
         * We want to intersect the shards for all AND queries since the query will only return true for
         * shards that will all make the tree return true.
         */

        // if the parent is an AndNode, we need to intersect keysToShards
        if(node instanceof AndNode) {
            for(Set<String> curShards : resultShards) {
                if(combinedSet == null)
                    combinedSet = curShards;
                else {
                    Set<String> intersected = Sets.intersection(combinedSet, curShards);
                    combinedSet = new HashSet<String>(intersected);
                }
            }

        /**
         * For OR nodes, we can just union the shards together because any one of the shards
         * will make the tree return true.
         */

        } else {
            for(Set<String> curShards : resultShards) {
                if(combinedSet == null)
                    combinedSet = Sets.newHashSet();
                combinedSet.addAll(curShards);
            }
        }

        return (combinedSet != null ? combinedSet : Sets.<String>newHashSet());
    }

    private Set<String> getShards(Leaf leaf) {
        AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

        /**
         * Range leaves are unbounded- that is, any number of shards within the beginning and ending shard
         * ranges given could have values that would evaluate these leaves to true. Because of this, we
         * can help eliminate shards which do not contain any values for the keys associated with
         * each leaf.
         */
        // hasKey and hasNotKey need special treatment since we don't know the aliases
        if (leaf instanceof HasLeaf || leaf instanceof HasNotLeaf || NodeUtils.isRangeLeaf(leaf) || leaf instanceof NotEqualsLeaf) {
            Set<AttributeIndexKey> tupleIndexKeys = accumuloKeyToAttributeIndexKey.get(kvLeaf.getKey());
            Set<String> unionedShards = new HashSet<String>();
            if (tupleIndexKeys == null) {
                if (leaf instanceof NegationLeaf)
                    return unionedShards;
            } else {
                for (AttributeIndexKey key : tupleIndexKeys) {
                    unionedShards.addAll(this.keysToShards.get(key));
                }
            }

            return unionedShards;

        /**
         * EqualsLeaf and other discrete leaves have the best ability to filter down shards as they are directly
         * indexed so we know exactly which shards will contain these values. It's still possible every shard does
         * contain the value- but at least we can be sure we'll get results from those shards.
         */
        } else {
            String alias = registry.getAlias(kvLeaf.getValue());
            String normalizedVal;
            normalizedVal = registry.encode(kvLeaf.getValue());

            AttributeIndexKey tupleIndexKey = new AttributeIndexKey(kvLeaf.getKey(), normalizedVal, alias);
            Set<String> leafShards = keysToShards.get(tupleIndexKey);

            if (leafShards == null)
                return Sets.newHashSet();

            return leafShards;
        }
    }
}
