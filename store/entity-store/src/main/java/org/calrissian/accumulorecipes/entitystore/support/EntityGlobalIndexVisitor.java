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

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.qfd.AttributeIndexKey;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.mango.criteria.domain.*;
import org.calrissian.mango.types.TypeEncoder;
import org.calrissian.mango.types.TypeRegistry;

import java.util.*;

import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex.INDEX_SEP;
import static org.calrissian.mango.criteria.support.NodeUtils.isRangeLeaf;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class EntityGlobalIndexVisitor implements GlobalIndexVisitor {

    private static final TypeRegistry<String> registry = LEXI_TYPES;

    private final BatchScanner indexScanner;
    private final EntityShardBuilder shardBuilder;

    private Map<AttributeIndexKey, Long> cardinalities = new HashMap<AttributeIndexKey, Long>();
    private Map<AttributeIndexKey, Set<String>> mappedShards = new HashMap<AttributeIndexKey, Set<String>>();

    private final Set<String> types;

    private Set<Leaf> leaves = new HashSet<Leaf>();

    public EntityGlobalIndexVisitor(BatchScanner indexScanner, EntityShardBuilder shardBuilder, Set<String> types) {
        this.indexScanner = indexScanner;
        this.shardBuilder = shardBuilder;
        this.types = types;
    }

    @Override
    public Map<AttributeIndexKey, Long> getCardinalities() {
        return cardinalities;
    }

    @Override
    public Map<AttributeIndexKey, Set<String>> getShards() {
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

            TypedTermLeaf typedTerm = (TypedTermLeaf) leaf;
            String alias = registry.getClassAlias(typedTerm.getType());

            for (String type : types) {

                String startShard = shardBuilder.buildShard(type, 0);
                String stopShard = shardBuilder.buildShard(type, shardBuilder.numPartitions());

                if (leaf instanceof HasLeaf || leaf instanceof HasNotLeaf) {
                    if (alias == null)
                        buildRangeForAllAliases(ranges, typedTerm.getTerm(), startShard, stopShard);
                    else
                        buildRangeForSingleAlias(ranges, typedTerm.getTerm(), alias, startShard, stopShard);
                } else if (isRangeLeaf(leaf) || leaf instanceof NotEqualsLeaf) {
                    buildRangeForSingleAlias(ranges, typedTerm.getTerm(), alias, startShard, stopShard);
                } else {
                    TermValueLeaf termValLeaf = (TermValueLeaf) leaf;
                    String normVal = registry.encode(termValLeaf.getValue());
                    ranges.add(
                            new Range(
                                    new Key(INDEX_V + INDEX_SEP + type + INDEX_SEP + alias + INDEX_SEP + termValLeaf.getTerm() + NULL_BYTE + normVal + NULL_BYTE + startShard),
                                    new Key(INDEX_V + INDEX_SEP + type + INDEX_SEP + alias + INDEX_SEP + termValLeaf.getTerm() + NULL_BYTE + normVal + NULL_BYTE + stopShard)
                            )
                    );

                }
            }

        }

        indexScanner.setRanges(ranges);

        for (Map.Entry<Key,Value> entry : indexScanner) {

            AttributeIndexKey key = new AttributeIndexKey(entry.getKey());
            Long cardinality = cardinalities.get(key);
            if (cardinality == null)
                cardinality = 0l;
            GlobalIndexValue value = new GlobalIndexValue(entry.getValue());
            cardinalities.put(key, cardinality + value.getCardinatlity());

            Set<String> shardsForKey = mappedShards.get(key);
            if (shardsForKey == null) {
                shardsForKey = new HashSet<String>();
                mappedShards.put(key, shardsForKey);
            }

            shardsForKey.add(key.getShard());
        }

        indexScanner.close();
    }


    private void buildRangeForSingleAlias(Collection<Range> ranges, String key, String alias, String startShard, String stopShard) {

        for(String type : types) {
            ranges.add(new Range(
                    new Key(INDEX_K + INDEX_SEP + type + INDEX_SEP + key + INDEX_SEP + alias + NULL_BYTE + startShard),
                    new Key(INDEX_K + INDEX_SEP + type + INDEX_SEP + key + INDEX_SEP + alias + NULL_BYTE + stopShard)
            ));
        }
    }

    private void buildRangeForAllAliases(Collection<Range> ranges, String key, String startShard, String stopShard) {
        for(TypeEncoder encoder : registry.getAllEncoders())
            buildRangeForSingleAlias(ranges, key, encoder.getAlias(), startShard, stopShard);
    }

    @Override
    public void visit(Leaf leaf) {
        leaves.add(leaf);
    }
}
