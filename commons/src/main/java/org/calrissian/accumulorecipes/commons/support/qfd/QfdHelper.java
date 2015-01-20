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
import static java.util.Collections.EMPTY_LIST;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope.majc;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_FI;
import static org.calrissian.accumulorecipes.commons.support.RowEncoderUtil.encodeRow;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.VISIBILITY;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.EvaluatingIterator;
import org.calrissian.accumulorecipes.commons.iterators.GlobalIndexCombiner;
import org.calrissian.accumulorecipes.commons.iterators.GlobalIndexExpirationFilter;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerdeFactory;
import org.calrissian.accumulorecipes.commons.support.metadata.SimpleMetadataSerdeFactory;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.entity.Entity;
import org.calrissian.mango.types.TypeRegistry;



public abstract class QfdHelper<T extends Entity> {


    private static final Kryo kryo = new Kryo();
    private final Connector connector;
    private final String indexTable;
    private final String shardTable;
    private final StoreConfig config;
    private final BatchWriter shardWriter;
    private final NodeToJexl nodeToJexl;
    private ShardBuilder<T> shardBuilder;
    private TypeRegistry<String> typeRegistry;
    private MetadataSerDe metadataSerDe;
    private MetadataSerdeFactory metadataSerdeFactory;

    private KeyValueIndex<T> keyValueIndex;

    public QfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
        ShardBuilder<T> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<T> keyValueIndex,
        MetadataSerdeFactory metadaSerdeFactory, NodeToJexl nodeToJexl)
        throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        checkNotNull(connector);
        checkNotNull(indexTable);
        checkNotNull(shardTable);
        checkNotNull(config);
        checkNotNull(shardBuilder);
        checkNotNull(typeRegistry);
        checkNotNull(keyValueIndex);
        checkNotNull(metadaSerdeFactory);

        this.connector = connector;
        this.indexTable = indexTable;
        this.shardTable = shardTable;
        this.typeRegistry = typeRegistry;
        this.config = config;
        this.shardBuilder = shardBuilder;
        this.typeRegistry = typeRegistry;
        this.keyValueIndex = keyValueIndex;
        this.nodeToJexl = nodeToJexl;

        this.metadataSerDe = metadaSerdeFactory.create();
        this.metadataSerdeFactory = metadaSerdeFactory;

        if (!connector.tableOperations().exists(this.indexTable)) {
            connector.tableOperations().create(this.indexTable);
            configureIndexTable(connector, this.indexTable);
        }

        if (connector.tableOperations().getIteratorSetting(this.indexTable, "cardinalities", majc) == null) {
            IteratorSetting setting = new IteratorSetting(10, "cardinalities", GlobalIndexCombiner.class);
            GlobalIndexCombiner.setCombineAllColumns(setting, true);
            connector.tableOperations().attachIterator(this.indexTable, setting, allOf(IteratorScope.class));
            IteratorSetting expirationFilter = new IteratorSetting(12, "expiration", GlobalIndexExpirationFilter.class);
            connector.tableOperations().attachIterator(this.indexTable, expirationFilter, allOf(IteratorScope.class));
        }

        if (!connector.tableOperations().exists(this.shardTable)) {
            connector.tableOperations().create(this.shardTable, false);
            configureShardTable(connector, this.shardTable);
        }

        initializeKryo(kryo);
        this.shardWriter = connector.createBatchWriter(shardTable, config.getMaxMemory(), config.getMaxLatency(),
            config.getMaxWriteThreads());
    }


    public QfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
        ShardBuilder<T> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<T> keyValueIndex, NodeToJexl nodeToJexl)
        throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, indexTable, shardTable, config, shardBuilder, typeRegistry, keyValueIndex, new SimpleMetadataSerdeFactory(), nodeToJexl);
    }

    public MetadataSerDe getMetadataSerDe() {
        return metadataSerDe;
    }

    public MetadataSerdeFactory getMetadataSerdeFactory() {
        return metadataSerdeFactory;
    }

    public static Kryo getKryo() {
        return kryo;
    }

    public void flush() throws Exception {
        shardWriter.flush();
        keyValueIndex.flush();
    }

    /**
     * Items get saved into a sharded table to parallelize queries & ingest.
     */
    public void save(Iterable<? extends T> items) {
        checkNotNull(items);

        Value value = new Value();
        Text forwardCF = new Text();
        Text forwardCQ = new Text();
        Text fieldIndexCF = new Text();
        Text fieldIndexCQ = new Text();

        try {

            for (T item : items) {

                //If there are no getTuples then don't write anything to the data store.
                if (item.size() > 0) {
                    String id = buildId(item);

                    String shardId = shardBuilder.buildShard(item);

                    Multimap<ColumnVisibility,Map.Entry<Key,Value>> visToKeyCache = ArrayListMultimap.create();
                    Mutation shardMutation = new Mutation(shardId);
                    for (Tuple tuple : item.getTuples()) {
                        String visibility = getVisibility(tuple.getMetadata(), "");
                        String aliasValue = typeRegistry.getAlias(tuple.getValue()) + ONE_BYTE +
                            typeRegistry.encode(tuple.getValue());

                        ColumnVisibility columnVisibility = new ColumnVisibility(visibility);

                        Map<String,Object> meta = tuple.getMetadata();

                        value.set(metadataSerDe.serialize(meta));

                        forwardCF.set(id);
                        forwardCQ.set(tuple.getKey() + NULL_BYTE + aliasValue);
                        fieldIndexCF.set(PREFIX_FI + NULL_BYTE + buildTupleKey(item, tuple.getKey()));
                        fieldIndexCQ.set(aliasValue + NULL_BYTE + id);

                        Key key = new Key(new Text(shardId), forwardCF, forwardCQ, columnVisibility, 0);
                        Value valuePart = new Value(value);
                        visToKeyCache.put(columnVisibility, Maps.immutableEntry(key, valuePart));

                        shardMutation.put(fieldIndexCF,
                            fieldIndexCQ,
                            columnVisibility,
                            value);
                    }

                  for(ColumnVisibility colVis : visToKeyCache.keySet()) {
                    Value row = encodeRow(visToKeyCache.get(colVis));
                    shardMutation.put(new Text(id), new Text(), colVis, row);
                  }
                  shardWriter.addMutation(shardMutation);
                }
            }
            keyValueIndex.indexKeyValues(items);

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CloseableIterable<T> query(BatchScanner scanner, GlobalIndexVisitor globalIndexVisitor, Set<String> types, Node query,
        Function<Map.Entry<Key, Value>, T> transform, Set<String> selectFields, Auths auths) {
        checkNotNull(query);
        checkNotNull(auths);

        QueryOptimizer optimizer = new QueryOptimizer(query, globalIndexVisitor, typeRegistry);

        if (NodeUtils.isEmpty(optimizer.getOptimizedQuery()))
            return wrap(EMPTY_LIST);

        String jexl = nodeToJexl.transform(types, optimizer.getOptimizedQuery());
        String originalJexl = nodeToJexl.transform(types, query);
        Set<String> shards = optimizer.getShards();

        Collection<Range> ranges = new HashSet<Range>();
        if(jexl.equals("()") || jexl.equals(""))
            ranges.add(new Range(END_BYTE));
        else
            for (String shard : shards)
                ranges.add(new Range(shard));


        scanner.setRanges(ranges);

        IteratorSetting setting = new IteratorSetting(16, getOptimizedQueryIteratorClass());
        setting.addOption(BooleanLogicIterator.QUERY_OPTION, originalJexl);
        setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

        if(selectFields != null)
            EvaluatingIterator.setSelectFields(setting, selectFields);

        scanner.addScanIterator(setting);

        return transform(closeableIterable(scanner), transform);
    }

    protected Class<? extends OptimizedQueryIterator> getOptimizedQueryIteratorClass() {
      return OptimizedQueryIterator.class;
    }

    public void shutdown() {
        try {
            getWriter().close();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Utility method to update the correct iterators to the index table.
     *
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected abstract void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

    /**
     * Utility method to update the correct iterators to the shardBuilder table.
     *
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected abstract void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException;

    protected abstract String buildId(T item);

    protected abstract String buildTupleKey(T item, String key);

    public BatchScanner buildIndexScanner(Authorizations auths) {
        return buildScanner(indexTable, auths);
    }

    public BatchScanner buildShardScanner(Authorizations auths) {
        return buildScanner(shardTable, auths);
    }

    private BatchScanner buildScanner(String table, Authorizations authorizations) {
        try {
            return connector.createBatchScanner(table, authorizations, config.getMaxQueryThreads());
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public ShardBuilder getShardBuilder() {
        return shardBuilder;
    }

    public Connector getConnector() {
        return connector;
    }

    public String getIndexTable() {
        return indexTable;
    }

    public String getShardTable() {
        return shardTable;
    }

    public StoreConfig getConfig() {
        return config;
    }

    public BatchWriter getWriter() {
        return shardWriter;
    }

    public TypeRegistry<String> getTypeRegistry() {
        return typeRegistry;
    }

    private static Function<Map<String,Object>, Map<String,Object>> removeVisFunction =
        new Function<Map<String,Object>,Map<String,Object>>() {
            @Override
            public Map<String,Object> apply(Map<String,Object> stringObjectMap) {
                stringObjectMap.remove(VISIBILITY);
                return stringObjectMap;
            }
        };

    private static class ObjectVisCacheKey {
        private final String vis;
        private final Object obj;

        private ObjectVisCacheKey(String vis, Object obj) {
            this.vis = vis;
            this.obj = obj;
        }

        public String getVis() {
            return vis;
        }

        public Object getObj() {
            return obj;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectVisCacheKey that = (ObjectVisCacheKey) o;
            if (obj != null ? !obj.equals(that.obj) : that.obj != null)
                return false;
            if (vis != null ? !vis.equals(that.vis) : that.vis != null)
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = vis != null ? vis.hashCode() : 0;
            result = 31 * result + (obj != null ? obj.hashCode() : 0);
            return result;
        }
    }

}
