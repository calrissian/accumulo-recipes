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
import static org.apache.accumulo.core.data.Range.exact;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.accumulorecipes.commons.support.Constants.DEFAULT_PARTITION_SIZE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_E;
import static org.calrissian.accumulorecipes.commons.util.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.MetadataExpirationFilter;
import org.calrissian.accumulorecipes.commons.iterators.SelectFieldsExtractor;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.EntityStore;
import org.calrissian.accumulorecipes.entitystore.support.EntityGlobalIndexVisitor;
import org.calrissian.accumulorecipes.entitystore.support.EntityQfdHelper;
import org.calrissian.accumulorecipes.entitystore.support.EntityShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.domain.entity.EntityIdentifier;
import org.calrissian.mango.types.TypeRegistry;

public class AccumuloEntityStore implements EntityStore {

    public static final String DEFAULT_IDX_TABLE_NAME = "entity_index";
    public static final String DEFAULT_SHARD_TABLE_NAME = "entity_shard";

    protected static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

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

        KeyValueIndex<Entity> keyValueIndex = new KeyValueIndex(connector, indexTable, shardBuilder, config, typeRegistry);

        this.shardBuilder = shardBuilder;
        helper = new EntityQfdHelper(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex);
    }

    @Override
    public void shutdown() throws MutationsRejectedException {
        helper.shutdown();
    }

    @Override
    public void save(Iterable<Entity> entities) {
        save(entities, true);
    }

    @Override
    public void save(Iterable<Entity> entities, boolean writeIndices) {
        helper.save(entities, writeIndices);
    }

    @Override
    public CloseableIterable<Entity> get(Collection<EntityIdentifier> typesAndIds, Set<String> selectFields, Auths auths) {
        checkNotNull(typesAndIds);
        checkNotNull(auths);
        try {

            BatchScanner scanner = helper.buildShardScanner(auths.getAuths());

            Collection<Range> ranges = new LinkedList<Range>();
            for (EntityIdentifier curIndex : typesAndIds) {
                String shardId = shardBuilder.buildShard(curIndex.getType(), curIndex.getId());
                ranges.add(exact(shardId, PREFIX_E + ONE_BYTE + curIndex.getType() + ONE_BYTE + curIndex.getId()));
            }

            scanner.setRanges(ranges);

            if (selectFields != null && selectFields.size() > 0) {
                IteratorSetting iteratorSetting = new IteratorSetting(16, SelectFieldsExtractor.class);
                SelectFieldsExtractor.setSelectFields(iteratorSetting, selectFields);
                scanner.addScanIterator(iteratorSetting);
            }

            IteratorSetting expirationFilter = new IteratorSetting(7, "metaExpiration", MetadataExpirationFilter.class);
            scanner.addScanIterator(expirationFilter);

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
    public CloseableIterable<Entity> get(Collection<EntityIdentifier> typesAndIds, Auths auths) {
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


            IteratorSetting expirationFilter = new IteratorSetting(7, "metaExpiration", MetadataExpirationFilter.class);
            scanner.addScanIterator(expirationFilter);


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


    public CloseableIterable<Pair<String,String>> uniqueKeys(String prefix, String type, Auths auths) {
        return helper.getKeyValueIndex().uniqueKeys(prefix, type, auths);
    }

    public CloseableIterable<Object> uniqueValuesForKey(String prefix, String type, String alias, String key, Auths auths) {
        return helper.getKeyValueIndex().uniqueValuesForKey(prefix, type, alias, key, auths);
    }

    public CloseableIterable<String> getTypes(String prefix, Auths auths) {
        return helper.getKeyValueIndex().getTypes(prefix, auths);
    }


    @Override
    public void flush() throws Exception {
        helper.flush();
    }

    protected EntityQfdHelper getHelper() {
        return helper;
    }

    public static class Builder {
        private final Connector connector;
        private String indexTable = DEFAULT_IDX_TABLE_NAME;
        private String shardTable = DEFAULT_SHARD_TABLE_NAME;
        private StoreConfig storeConfig = DEFAULT_STORE_CONFIG;
        private TypeRegistry<String> typeRegistry = LEXI_TYPES;
        private EntityShardBuilder shardBuilder = DEFAULT_SHARD_BUILDER;

        public Builder(Connector connector) {
            checkNotNull(connector);
            this.connector = connector;
        }

        public Builder setIndexTable(String indexTable) {
            checkNotNull(indexTable);
            this.indexTable = indexTable;
            return this;
        }

        public Builder setShardTable(String shardTable) {
            checkNotNull(shardTable);
            this.shardTable = shardTable;
            return this;
        }

        public Builder setStoreConfig(StoreConfig storeConfig) {
            checkNotNull(storeConfig);
            this.storeConfig = storeConfig;
            return this;
        }

        public Builder setTypeRegistry(TypeRegistry<String> typeRegistry) {
            checkNotNull(typeRegistry);
            this.typeRegistry = typeRegistry;
            return this;
        }

        public Builder setShardBuilder(EntityShardBuilder shardBuilder) {
            checkNotNull(shardBuilder);
            this.shardBuilder = shardBuilder;
            return this;
        }

        public AccumuloEntityStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            return new AccumuloEntityStore(connector, indexTable, shardTable, shardBuilder,storeConfig, typeRegistry);
        }
    }

}
