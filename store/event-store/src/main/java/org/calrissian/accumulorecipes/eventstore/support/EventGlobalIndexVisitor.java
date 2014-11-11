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
package org.calrissian.accumulorecipes.eventstore.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.criteria.CardinalityKey;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.calrissian.accumulorecipes.eventstore.support.shard.EventShardBuilder;
import org.calrissian.mango.criteria.domain.AbstractKeyValueLeaf;
import org.calrissian.mango.criteria.domain.HasLeaf;
import org.calrissian.mango.criteria.domain.HasNotLeaf;
import org.calrissian.mango.criteria.domain.Leaf;
import org.calrissian.mango.criteria.domain.ParentNode;
import org.calrissian.mango.types.TypeRegistry;

import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.mango.criteria.support.NodeUtils.isRangeLeaf;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EventGlobalIndexVisitor implements GlobalIndexVisitor {

    private static TypeRegistry<String> registry = LEXI_TYPES;

    private Date start;
    private Date end;
    private BatchScanner indexScanner;
    private EventShardBuilder shardBuilder;

    private Set<String> shards = new HashSet<String>();
    private Map<CardinalityKey, Long> cardinalities = new HashMap<CardinalityKey, Long>();

    private Set<Leaf> leaves = new HashSet<Leaf>();

    public EventGlobalIndexVisitor(Date start, Date end, BatchScanner indexScanner, EventShardBuilder shardBuilder) {
        this.start = start;
        this.end = end;
        this.indexScanner = indexScanner;
        this.shardBuilder = shardBuilder;
    }

    @Override
    public Map<CardinalityKey, Long> getCardinalities() {
        return cardinalities;
    }

    @Override
    public Set<String> getShards() {
        return shards;
    }

    @Override
    public void exec() {

        Collection<Range> ranges = new ArrayList<Range>();
        for (Leaf leaf : leaves) {

            AbstractKeyValueLeaf kvLeaf = (AbstractKeyValueLeaf) leaf;

            String alias = registry.getAlias(kvLeaf.getValue());
            String startShard = shardBuilder.buildShard(start.getTime(), 0);
            String stopShard = shardBuilder.buildShard(end.getTime(), shardBuilder.numPartitions() - 1) + END_BYTE;

            if (isRangeLeaf(leaf) || leaf instanceof HasLeaf || leaf instanceof HasNotLeaf) {
                ranges.add(
                        new Range(
                                new Key(INDEX_K + "_" + kvLeaf.getKey() + "__" + alias + NULL_BYTE + startShard),
                                new Key(INDEX_K + "_" + kvLeaf.getKey() + "__" + alias + NULL_BYTE + stopShard)
                        )
                );
            } else {

                String normVal = registry.encode(kvLeaf.getValue());
                ranges.add(
                        new Range(
                                new Key(INDEX_V + "_" + alias + "__" + kvLeaf.getKey() + NULL_BYTE + normVal + NULL_BYTE + startShard),
                                new Key(INDEX_V + "_" + alias + "__" + kvLeaf.getKey() + NULL_BYTE + normVal + NULL_BYTE + stopShard)
                        )
                );

            }
        }

        indexScanner.setRanges(ranges);

        for (Map.Entry<Key, Value> entry : indexScanner) {

            CardinalityKey key = new EventCardinalityKey(entry.getKey());
            Long cardinality = cardinalities.get(key);
            if (cardinality == null)
                cardinality = 0l;
            GlobalIndexValue value = new GlobalIndexValue(entry.getValue());
            cardinalities.put(key, cardinality + value.getCardinatlity());
            shards.add(key.getShard());
        }

        indexScanner.close();
    }

    @Override
    public void begin(ParentNode parentNode) {
    }

    @Override
    public void end(ParentNode parentNode) {

    }

    @Override
    public void visit(Leaf leaf) {
        leaves.add(leaf);
    }
}
