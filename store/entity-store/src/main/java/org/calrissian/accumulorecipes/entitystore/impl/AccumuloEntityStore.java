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
package org.calrissian.accumulorecipes.entitystore.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.accumulo.core.data.Range.exact;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.accumulorecipes.commons.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.calrissian.accumulorecipes.commons.support.Constants.INDEX_K;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.FirstEntryInColumnIterator;
import org.calrissian.accumulorecipes.commons.iterators.SelectFieldsExtractor;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.support.EntityCardinalityKey;
import org.calrissian.accumulorecipes.entitystore.support.EntityGlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.support.EntityKeyValueIndex;
import org.calrissian.accumulorecipes.entitystore.support.EntityQfdHelper;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIndex;
import org.calrissian.mango.types.TypeRegistry;

public class AccumuloEntityStore implements EntityStore {

    public static final String DEFAULT_IDX_TABLE_NAME = "entity_index";
    public static final String DEFAULT_SHARD_TABLE_NAME = "entity_shard";

    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

    public static final EntityShardBuilder DEFAULT_SHARD_BUILDER = new EntityShardBuilder(DEFAULT_PARTITION_SIZE);

    private final EntityShardBuilder shardBuilder;
    private final EntityQfdHelper helper;

    public AccumuloEntityStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG, LEXI_TYPES);
    }

    public AccumuloEntityStore(Connector connector, String indexTable, String shardTable, EntityShardBuilder shardBuilder, StoreConfig config, TypeRegistry<String> typeRegistry)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector);
        checkNotNull(indexTable);
        checkNotNull(shardTable);
        checkNotNull(config);
        checkNotNull(typeRegistry);
        checkNotNull(shardBuilder);

        KeyValueIndex<Entity> keyValueIndex = new EntityKeyValueIndex(connector, indexTable, shardBuilder, config, typeRegistry);

        this.shardBuilder = shardBuilder;
        helper = new EntityQfdHelper(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
    }

    @Override
    public void shutdown() throws MutationsRejectedException {
        helper.shutdown();
    }

    @Override
    public void save(Iterable<? extends Entity> entities) {
        helper.save(entities);
    }

    @Override
    public CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Set<String> selectFields, Auths auths) {
        checkNotNull(typesAndIds);
        checkNotNull(auths);
        try {

            BatchScanner scanner = helper.buildShardScanner(auths.getAuths());

            Collection<Range> ranges = new LinkedList<Range>();
            for (EntityIndex curIndex : typesAndIds) {
                String shardId = shardBuilder.buildShard(curIndex.getType(), curIndex.getId());
                ranges.add(exact(shardId, PREFIX_E + ONE_BYTE + curIndex.getType() + ONE_BYTE + curIndex.getId()));
            }

            scanner.setRanges(ranges);

            if (selectFields != null && selectFields.size() > 0) {
                IteratorSetting iteratorSetting = new IteratorSetting(16, SelectFieldsExtractor.class);
                SelectFieldsExtractor.setSelectFields(iteratorSetting, selectFields);
                scanner.addScanIterator(iteratorSetting);
            }

            IteratorSetting setting = new IteratorSetting(18, WholeColumnFamilyIterator.class);
            scanner.addScanIterator(setting);


            return transform(closeableIterable(scanner), helper.buildWholeColFXform());
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterable<Entity> get(List<EntityIndex> typesAndIds, Auths auths) {
        return get(typesAndIds, null, auths);
    }

    @Override
    public CloseableIterable<Entity> getAllByType(Set<String> types, Set<String> selectFields, Auths auths) {
        checkNotNull(types);
        checkNotNull(auths);
        try {

            BatchScanner scanner = helper.buildShardScanner(auths.getAuths());

            Collection<Range> ranges = new LinkedList<Range>();
            for (String type : types) {
                Set<Text> shards = shardBuilder.buildShardsForTypes(singleton(type));
                for (Text shard : shards)
                    ranges.add(prefix(shard.toString(), PREFIX_E + ONE_BYTE + type));
            }

            scanner.setRanges(ranges);

            if (selectFields != null && selectFields.size() > 0) {
                IteratorSetting iteratorSetting = new IteratorSetting(16, SelectFieldsExtractor.class);
                SelectFieldsExtractor.setSelectFields(iteratorSetting, selectFields);
                scanner.addScanIterator(iteratorSetting);
            }

            IteratorSetting setting = new IteratorSetting(18, WholeColumnFamilyIterator.class);
            scanner.addScanIterator(setting);


            return transform(closeableIterable(scanner), helper.buildWholeColFXform());
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public CloseableIterable<Entity> getAllByType(Set<String> types, Auths auths) {
        return getAllByType(types, null, auths);
    }

    @Override
    public CloseableIterable<Entity> query(Set<String> types, Node query, Set<String> selectFields, Auths auths) {

        checkNotNull(types);
        checkNotNull(query);
        checkNotNull(auths);

        checkArgument(types.size() > 0);

        BatchScanner indexScanner = helper.buildIndexScanner(auths.getAuths());
        GlobalIndexVisitor globalIndexVisitor = new EntityGlobalIndexVisitor(indexScanner, shardBuilder, types);

        BatchScanner scanner = helper.buildShardScanner(auths.getAuths());
        CloseableIterable<Entity> entities = helper.query(scanner, globalIndexVisitor, types, query,
                helper.buildQueryXform(), selectFields, auths);
        indexScanner.close();

        return entities;
    }

    @Override public CloseableIterable<Entity> query(Set<String> types, Node query, Auths auths) {
        return query(types, query, null, auths);
    }

    @Override
    public CloseableIterable<Pair<String, String>> keys(String type, Auths auths) {

        checkNotNull(type);
        checkNotNull(auths);

        BatchScanner scanner = helper.buildIndexScanner(auths.getAuths());
        IteratorSetting setting = new IteratorSetting(15, FirstEntryInColumnIterator.class);
        scanner.addScanIterator(setting);
        scanner.setRanges(singletonList(Range.prefix(type + "_" + INDEX_K + "_")));

        return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, Pair<String, String>>() {
            @Override
            public Pair<String, String> apply(Map.Entry<Key, Value> keyValueEntry) {
                EntityCardinalityKey key = new EntityCardinalityKey(keyValueEntry.getKey());
                return new Pair<String, String>(key.getKey(), key.getAlias());
            }
        });
    }


    @Override
    public void flush() throws Exception {
        helper.flush();
    }

    protected EntityQfdHelper getHelper() {
        return helper;
    }
}
