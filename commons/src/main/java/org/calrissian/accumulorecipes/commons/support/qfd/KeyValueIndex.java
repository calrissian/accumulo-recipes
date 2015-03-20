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
package org.calrissian.accumulorecipes.commons.support.qfd;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitByWholeSeparatorPreserveAllTokens;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_V;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.util.Scanners.closeableIterable;
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
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.GlobalIndexTypesIterator;
import org.calrissian.accumulorecipes.commons.iterators.GlobalIndexUniqueKeyValueIterator;
import org.calrissian.accumulorecipes.commons.support.Constants;
import org.calrissian.accumulorecipes.commons.support.attribute.Metadata;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;

public class KeyValueIndex<T extends Entity> {

    public static final String INDEX_SEP = "__";

    private final ShardBuilder<T> shardBuilder;
    private final TypeRegistry<String> typeRegistry;
    private final String indexTable;
    private final Connector connector;
    private final BatchWriter writer;
    private final StoreConfig config;

    private static final Text EMPTY_TEXT = new Text();

    public KeyValueIndex(Connector connector, String indexTable, ShardBuilder<T> shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.shardBuilder = shardBuilder;
        this.typeRegistry = typeRegistry;

        this.indexTable = indexTable;
        this.connector = connector;

        this.config = config;

        if(!connector.tableOperations().exists(indexTable))
            connector.tableOperations().create(indexTable);

        writer = connector.createBatchWriter(indexTable, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    public void indexKeyValues(Iterable<T> items) {

        Map<String, Long> indexCache = new HashMap<String, Long>();
        Map<String, Long> expirationCache = new HashMap<String, Long>();

        for (T item : items) {
            String shardId = shardBuilder.buildShard(item);
            for (Attribute attribute : item.getAttributes()) {
                String[] strings = new String[]{
                    shardId,
                    attribute.getKey(),
                    typeRegistry.getAlias(attribute.getValue()),
                    typeRegistry.encode(attribute.getValue()),
                    getVisibility(attribute, ""),
                    item.getType()
                };

                String cacheKey = join(strings, ONE_BYTE);
                Long count = indexCache.get(cacheKey);
                if (count == null)
                    count = 0l;

                Long expiration = expirationCache.get(cacheKey);
                if(expiration == null)
                    expiration = 0l;

                Long curExpiration = Metadata.Expiration.getExpiration(attribute.getMetadata(), -1);
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

    /**
     * Returns all the unique keys for the given event type for the given prefix. Unique key is defined as a unique key + data type alias
     * @param connector
     * @param indexTable
     * @param prefix
     * @param type
     * @param maxQueryThreads
     * @param auths
     * @return
     */
    public static CloseableIterable<Pair<String,String>> uniqueKeys(Connector connector, String indexTable, String prefix, String type, int maxQueryThreads, Auths auths) {

        checkNotNull(prefix);
        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(indexTable, auths.getAuths(), maxQueryThreads);
            IteratorSetting setting = new IteratorSetting(15, GlobalIndexUniqueKeyValueIterator.class);
            scanner.addScanIterator(setting);

            scanner.setRanges(singletonList(
                    new Range(INDEX_K + INDEX_SEP + type + INDEX_SEP + prefix + Constants.NULL_BYTE,
                        INDEX_K + INDEX_SEP + type + INDEX_SEP + prefix + END_BYTE))
            );

            return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, Pair<String, String>>() {
                @Override
                public Pair<String, String> apply(Map.Entry<Key, Value> keyValueEntry) {
                    AttributeIndexKey key = new AttributeIndexKey(keyValueEntry.getKey());
                    return new Pair(key.getKey(), key.getAlias());
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns all the unique values for the given event type for the given key, alias, and prefix. It's possible that a large
     * set of unique values could take a long time to return.
     * @param connector
     * @param indexTable
     * @param prefix
     * @param type
     * @param alias
     * @param key
     * @param maxQueryThreads
     * @param auths
     * @return
     */
    public CloseableIterable<Object> uniqueValuesForKey(String prefix, String type, String alias, String key, Auths auths) {

        checkNotNull(prefix);
        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(indexTable, auths.getAuths(), config.getMaxQueryThreads());
            IteratorSetting setting = new IteratorSetting(15, GlobalIndexUniqueKeyValueIterator.class);
            scanner.addScanIterator(setting);

            scanner.setRanges(singletonList(
                    new Range(INDEX_V + INDEX_SEP + type + INDEX_SEP + alias + INDEX_SEP + key + NULL_BYTE + prefix + NULL_BYTE,
                        INDEX_V + INDEX_SEP + type + INDEX_SEP + alias + INDEX_SEP + key + NULL_BYTE + prefix + END_BYTE))
            );

            return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, Object>() {
                @Override
                public Object apply(Map.Entry<Key, Value> keyValueEntry) {
                    AttributeIndexKey key = new AttributeIndexKey(keyValueEntry.getKey());
                    return typeRegistry.decode(key.getAlias(), key.getNormalizedValue());
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns all the unique event types in the event store.
     * @param auths
     * @return
     */
    public CloseableIterable<String> getTypes(String prefix, Auths auths) {

        checkNotNull(auths);

        try {
            BatchScanner scanner = connector.createBatchScanner(indexTable, auths.getAuths(), config.getMaxQueryThreads());
            IteratorSetting setting = new IteratorSetting(15, GlobalIndexTypesIterator.class);

            scanner.addScanIterator(setting);
            scanner.setRanges(singletonList(new Range(INDEX_K + INDEX_SEP + prefix, INDEX_K + INDEX_SEP + prefix + "\uffff")));

            return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, String>() {
                @Override
                public String apply(Map.Entry<Key, Value> keyValueEntry) {
                    String[] parts = splitByWholeSeparatorPreserveAllTokens(keyValueEntry.getKey().getRow().toString(), INDEX_SEP);
                    return parts[1];
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() throws Exception {
        writer.flush();
    }

    public void shutdown() throws Exception {
        writer.close();
    }
}
