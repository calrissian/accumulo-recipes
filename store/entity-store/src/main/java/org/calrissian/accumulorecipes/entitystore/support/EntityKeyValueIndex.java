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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.support.tuple.Metadata;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;

public class EntityKeyValueIndex implements KeyValueIndex<Entity> {

    private final ShardBuilder<Entity> shardBuilder;
    private final TypeRegistry<String> typeRegistry;

    private final BatchWriter writer;

    public EntityKeyValueIndex(Connector connector, String indexTable, ShardBuilder<Entity> shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry) throws TableNotFoundException {
        this.shardBuilder = shardBuilder;
        this.typeRegistry = typeRegistry;

        writer = connector.createBatchWriter(indexTable, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    @Override
    public void indexKeyValues(Iterable<? extends Entity> items) {

        Map<String, Long> indexCache = new HashMap<String, Long>();
        Map<String, Long> expirationCache = new HashMap<String, Long>();

        for (Entity entity : items) {
            String shardId = shardBuilder.buildShard(entity);
            for (Tuple tuple : entity.getTuples()) {
                String[] strings = new String[]{
                        entity.getType(),
                        shardId,
                        tuple.getKey(),
                        typeRegistry.getAlias(tuple.getValue()),
                        typeRegistry.encode(tuple.getValue()),
                        getVisibility(tuple, ""),
                };

                String cacheKey = join(strings, ONE_BYTE);
                Long count = indexCache.get(cacheKey);
                if (count == null)
                    count = 0l;

                Long expiration = expirationCache.get(cacheKey);
                if(expiration == null)
                    expiration = 0l;

                Long curExpiration = Metadata.Expiration.getExpiration(tuple.getMetadata(), -1);
                if(curExpiration == -1)
                    expiration = -1l;
                else
                    expiration = Math.max(expiration, curExpiration);


                indexCache.put(cacheKey, ++count);
                expirationCache.put(cacheKey, expiration);
            }
        }

        for (Map.Entry<String, Long> indexCacheKey : indexCache.entrySet()) {

            String[] indexParts = splitPreserveAllTokens(indexCacheKey.getKey(), ONE_BYTE);

            String entityType = indexParts[0];
            String shard = indexParts[1];
            String key = indexParts[2];
            String alias = indexParts[3];
            String normalizedVal = indexParts[4];
            String vis = indexParts[5];

            Mutation keyMutation = new Mutation(entityType + "_" + INDEX_K + "_" + key);
            Mutation valueMutation = new Mutation(entityType + "_" + INDEX_V + "_" + alias + "__" + normalizedVal);

            GlobalIndexValue indexValue = new GlobalIndexValue(indexCacheKey.getValue(), expirationCache.get(indexCacheKey.getKey()));
            Value value = indexValue.toValue();
            keyMutation.put(new Text(alias), new Text(shard), new ColumnVisibility(vis), value);
            valueMutation.put(new Text(key), new Text(shard), new ColumnVisibility(vis), value);
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
