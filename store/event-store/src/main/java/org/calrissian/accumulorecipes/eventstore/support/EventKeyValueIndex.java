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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.ShardBuilder;
import org.calrissian.accumulorecipes.commons.support.tuple.Metadata;
import org.calrissian.accumulorecipes.eventstore.support.iterators.EventGlobalIndexUniqueKVIterator;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

public class EventKeyValueIndex implements KeyValueIndex<Event> {

    public static final String INDEX_SEP = "__";

    private final ShardBuilder<Event> shardBuilder;
    private final TypeRegistry<String> typeRegistry;
    private final String indexTable;
    private final Connector connector;
    private final BatchWriter writer;
    private final StoreConfig config;

    private static final Text EMPTY_TEXT = new Text();

    public EventKeyValueIndex(Connector connector, String indexTable, ShardBuilder<Event> shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.shardBuilder = shardBuilder;
        this.typeRegistry = typeRegistry;

        this.indexTable = indexTable;
        this.connector = connector;

        this.config = config;

        if(!connector.tableOperations().exists(indexTable))
            connector.tableOperations().create(indexTable);

        writer = connector.createBatchWriter(indexTable, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    @Override
    public void indexKeyValues(Iterable<? extends Event> items) {

        Map<String, Long> indexCache = new HashMap<String, Long>();
        Map<String, Long> expirationCache = new HashMap<String, Long>();

        for (Event item : items) {
            String shardId = shardBuilder.buildShard(item);
            for (Tuple tuple : item.getTuples()) {
                String[] strings = new String[]{
                    shardId,
                    tuple.getKey(),
                    typeRegistry.getAlias(tuple.getValue()),
                    typeRegistry.encode(tuple.getValue()),
                    getVisibility(tuple, ""),
                    item.getType()
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
            String alias = indexParts[2];
            String key = indexParts[1];
            String shard = indexParts[0];
            String vis = indexParts[4];
            String val = indexParts[3];
            String type = indexParts[5];

            Mutation keyMutation = new Mutation(INDEX_K + INDEX_SEP + type + INDEX_SEP + key + INDEX_SEP + alias + NULL_BYTE + shard);
            Mutation valueMutation = new Mutation(INDEX_V + INDEX_SEP + type + INDEX_SEP + alias + INDEX_SEP + key + NULL_BYTE + val + NULL_BYTE + shard);

            Long expiration = expirationCache.get(indexCacheKey.getKey());
            Value value = new GlobalIndexValue(indexCacheKey.getValue(), expiration).toValue();
            keyMutation.put(EMPTY_TEXT, EMPTY_TEXT, new ColumnVisibility(vis), value);
            valueMutation.put(EMPTY_TEXT, EMPTY_TEXT, new ColumnVisibility(vis), value);
            try {
                writer.addMutation(keyMutation);
                writer.addMutation(valueMutation);
            } catch (MutationsRejectedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public CloseableIterable<Pair<String,String>> uniqueKeys(String prefix, String type, Auths auths) {
        return uniqueKeys(connector, indexTable, prefix, type, config.getMaxQueryThreads(), auths);
    }

    public static CloseableIterable<Pair<String,String>> uniqueKeys(Connector connector, String indexTable, String prefix, String type, int maxQueryThreads, Auths auths) {

        checkNotNull(prefix);
        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(indexTable, auths.getAuths(), maxQueryThreads);
            IteratorSetting setting = new IteratorSetting(15, EventGlobalIndexUniqueKVIterator.class);
            scanner.addScanIterator(setting);

            scanner.setRanges(singletonList(
                    new Range(INDEX_K + INDEX_SEP + type + INDEX_SEP + prefix + Constants.NULL_BYTE,
                        INDEX_K + INDEX_SEP + type + INDEX_SEP + prefix + Constants.END_BYTE))
            );

            return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, Pair<String, String>>() {
                @Override
                public Pair<String, String> apply(Map.Entry<Key, Value> keyValueEntry) {
                    EventCardinalityKey key = new EventCardinalityKey(keyValueEntry.getKey());
                    return new Pair(key.getKey(), key.getAlias());
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CloseableIterable<String> getTypes(Auths auths) {

        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(indexTable, auths.getAuths(), config.getMaxQueryThreads());
            IteratorSetting setting = new IteratorSetting(15, EventGlobalIndexTypesIterator.class);
            scanner.addScanIterator(setting);

            scanner.setRanges(singletonList(new Range(INDEX_K + INDEX_SEP, INDEX_K + INDEX_SEP + "\uffff")));

            return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, String>() {
                @Override
                public String apply(Map.Entry<Key, Value> keyValueEntry) {
                    String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(keyValueEntry.getKey().getRow().toString(), INDEX_SEP);
                    return parts[1];
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws Exception {
        writer.flush();
    }

    @Override
    public void shutdown() throws Exception {
        writer.close();
    }
}
