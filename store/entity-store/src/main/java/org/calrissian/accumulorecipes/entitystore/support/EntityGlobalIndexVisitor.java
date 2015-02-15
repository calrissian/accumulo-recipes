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
package org.calrissian.accumulorecipes.entitystore.support;

import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.mango.criteria.support.NodeUtils.isRangeLeaf;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.criteria.TupleIndexKey;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.HasLeaf;
import org.calrissian.mango.criteria.domain.HasNotLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.NotEqualsLeaf;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.types.TypeRegistry;

public class EntityGlobalIndexVisitor implements GlobalIndexVisitor {

    private static TypeRegistry<String> registry = LEXI_TYPES;

    private BatchScanner indexScanner;
    private EntityShardBuilder shardBuilder;

    private Set<String> shards = new HashSet<String>();
    private Map<TupleIndexKey, Long> cardinalities = new HashMap<TupleIndexKey, Long>();
    private Map<TupleIndexKey, Set<String>> mappedShards = new HashMap<TupleIndexKey, Set<String>>();

    private Set<String> types;

    private Set<Leaf> leaves = new HashSet<Leaf>();

    public EntityGlobalIndexVisitor(BatchScanner indexScanner, EntityShardBuilder shardBuilder, Set<String> types) {
        this.indexScanner = indexScanner;
        this.shardBuilder = shardBuilder;
        this.types = types;
    }

    @Override
    public Map<TupleIndexKey, Long> getCardinalities() {
        return cardinalities;
    }

    @Override
    public Map<TupleIndexKey, Set<String>> getShards() {
        return mappedShards;
    }

    @Override
    public void begin(ParentNode parentNode) {
    }

    @Override
    public void end(ParentNode parentNode) {
    }

    public void exec() {
        Collection<Range> ranges = new ArrayList<Range>();
        for (Leaf leaf : leaves) {

            AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

            String alias = registry.getAlias(kvLeaf.getValue());

            for (String type : types) {
                if (isRangeLeaf(leaf) || leaf instanceof HasLeaf || leaf instanceof HasNotLeaf || leaf instanceof NotEqualsLeaf) {
                    if (alias != null)
                        ranges.add(prefix(type + "_" + INDEX_K + "_" + kvLeaf.getKey(), alias));
                    else
                        ranges.add(prefix(type + "_" + INDEX_K + "_" + kvLeaf.getKey()));
                } else {
                    String normVal = registry.encode(kvLeaf.getValue());
                    ranges.add(prefix(type + "_" + INDEX_V + "_" + alias + "__" + normVal, kvLeaf.getKey()));
                }
            }
        }

        indexScanner.setRanges(ranges);

        for (Map.Entry<Key, Value> entry : indexScanner) {

            TupleIndexKey key = new EntityCardinalityKey(entry.getKey());
            Long cardinality = cardinalities.get(key);
            if (cardinality == null)
                cardinality = 0l;
            long newCardinality = new GlobalIndexValue(entry.getValue()).getCardinatlity();
            cardinalities.put(key, cardinality + newCardinality);

            Set<String> shardsForKey = mappedShards.get(key);
            if(shardsForKey == null) {
                shardsForKey = Sets.newHashSet();
                mappedShards.put(key, shardsForKey);
            }

            shardsForKey.add(key.getShard());
        }

        indexScanner.close();
    }

    @Override
    public void visit(Leaf leaf) {
        leaves.add(leaf);
    }
}
