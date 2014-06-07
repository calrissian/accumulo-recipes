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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;

public class EventKeyValueIndex implements KeyValueIndex<Event> {

    private ShardBuilder<Event> shardBuilder;
    private TypeRegistry<String> typeRegistry;

    private BatchWriter writer;

    public EventKeyValueIndex(Connector connector, String indexTable, ShardBuilder<Event> shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry) throws TableNotFoundException {
        this.shardBuilder = shardBuilder;
        this.typeRegistry = typeRegistry;

        writer = connector.createBatchWriter(indexTable, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    @Override
    public void indexKeyValues(Iterable<? extends Event> items) {

        Map<String, Long> indexCache = new HashMap<String, Long>();

        for (Event item : items) {
            String shardId = shardBuilder.buildShard(item);
            for (Tuple tuple : item.getTuples()) {
                String[] strings = new String[]{
                        shardId,
                        tuple.getKey(),
                        typeRegistry.getAlias(tuple.getValue()),
                        typeRegistry.encode(tuple.getValue()),
                        getVisibility(tuple, "")
                };

                String cacheKey = join(strings, INNER_DELIM);
                Long count = indexCache.get(cacheKey);
                if (count == null)
                    count = 0l;
                indexCache.put(cacheKey, ++count);
            }

            String[] idIndex = new String[]{
                    shardBuilder.buildShard(item),
                    "@id",
                    "string",
                    item.getId(),
                    ""
            };

            indexCache.put(join(idIndex, INNER_DELIM), 1l);

        }

        for (Map.Entry<String, Long> indexCacheKey : indexCache.entrySet()) {

            String[] indexParts = splitPreserveAllTokens(indexCacheKey.getKey(), INNER_DELIM);
            Mutation keyMutation = new Mutation(INDEX_K + "_" + indexParts[1]);
            Mutation valueMutation = new Mutation(INDEX_V + "_" + indexParts[2] + "__" + indexParts[3]);

            Value value = new Value(Long.toString(indexCacheKey.getValue()).getBytes());
            keyMutation.put(new Text(indexParts[2]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), value);
            valueMutation.put(new Text(indexParts[1]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), value);
            try {
                writer.addMutation(keyMutation);
                writer.addMutation(valueMutation);
            } catch (MutationsRejectedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void commit() throws Exception {
        writer.flush();
    }

    @Override
    public void shutdown() throws Exception {
        writer.close();
    }
}