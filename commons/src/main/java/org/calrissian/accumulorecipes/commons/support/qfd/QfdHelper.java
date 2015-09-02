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
import static java.lang.Math.min;
import static java.util.Collections.EMPTY_LIST;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope.majc;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.ONE_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.PREFIX_FI;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.VISIBILITY;
import static org.calrissian.accumulorecipes.commons.support.attribute.Metadata.Visiblity.getVisibility;
import static org.calrissian.accumulorecipes.commons.util.RowEncoderUtil.encodeRowSimple;
import static org.calrissian.accumulorecipes.commons.util.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;
import static org.calrissian.mango.criteria.support.NodeUtils.isEmpty;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
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
import org.calrissian.accumulorecipes.commons.iterators.EmptyEncodedRowFilter;
import org.calrissian.accumulorecipes.commons.iterators.EvaluatingIterator;
import org.calrissian.accumulorecipes.commons.iterators.FieldIndexExpirationFilter;
import org.calrissian.accumulorecipes.commons.iterators.GlobalIndexCombiner;
import org.calrissian.accumulorecipes.commons.iterators.GlobalIndexExpirationFilter;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.attribute.Metadata;
import org.calrissian.accumulorecipes.commons.support.attribute.metadata.MetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.attribute.metadata.SimpleMetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.QueryPlanner;
import org.calrissian.accumulorecipes.commons.support.qfd.planner.visitors.GlobalIndexVisitor;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
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
    private MetadataSerDe metadataSerDe = new SimpleMetadataSerDe();

    private KeyValueIndex<T> keyValueIndex;

    public QfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
        ShardBuilder<T> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<T> keyValueIndex, NodeToJexl nodeToJexl)
        throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        checkNotNull(connector);
        checkNotNull(indexTable);
        checkNotNull(shardTable);
        checkNotNull(config);
        checkNotNull(shardBuilder);
        checkNotNull(typeRegistry);
        checkNotNull(keyValueIndex);

        this.connector = connector;
        this.indexTable = indexTable;
        this.shardTable = shardTable;
        this.typeRegistry = typeRegistry;
        this.config = config;
        this.shardBuilder = shardBuilder;
        this.typeRegistry = typeRegistry;
        this.keyValueIndex = keyValueIndex;
        this.nodeToJexl = nodeToJexl;

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

            Set<IteratorScope> scopes = Sets.newHashSet(IteratorScope.majc, IteratorScope.minc);

            connector.tableOperations().create(this.shardTable, true);  // we want the shard table to be idempotent
            configureShardTable(connector, this.shardTable);
            IteratorSetting expirationFilter = new IteratorSetting(6, "fiExpiration", FieldIndexExpirationFilter.class);
            connector.tableOperations().attachIterator(this.shardTable, expirationFilter, allOf(IteratorScope.class));
            IteratorSetting emptyDataFilter = new IteratorSetting(8, "emptyFilter", EmptyEncodedRowFilter.class);
            connector.tableOperations().attachIterator(this.shardTable, emptyDataFilter, Sets.newEnumSet(scopes, IteratorScope.class));
        }

        initializeKryo(kryo);
        this.shardWriter = connector.createBatchWriter(shardTable, config.getMaxMemory(), config.getMaxLatency(),
            config.getMaxWriteThreads());
    }


    public static Kryo getKryo() {
        return kryo;
    }

    public void flush() throws Exception {
        shardWriter.flush();
        keyValueIndex.flush();
    }

    public MetadataSerDe getMetadataSerDe() {
        return metadataSerDe;
    }

    /**
     * Items get saved into a sharded table to parallelize queries & ingest.
     */
    public void save(Iterable<T> items) {
        checkNotNull(items);

        Value shardVal = new Value();
        Value fiVal = new Value();
        Text forwardCF = new Text();
        Text forwardCQ = new Text();
        Text fieldIndexCF = new Text();
        Text fieldIndexCQ = new Text();

        try {

            for (T item : items) {

                //If there are no getAttributes then don't write anything to the data store.
                if (item.size() > 0) {
                    String id = buildId(item);

                    String shardId = shardBuilder.buildShard(item);

                    Multimap<ColumnVisibility,Map.Entry<Key,Value>> visToKeyCache = ArrayListMultimap.create();
                    Mutation shardMutation = new Mutation(shardId);

                    long minExpiration = Long.MAX_VALUE;
                    for (Attribute attribute : item.getAttributes()) {
                        String visibility = getVisibility(attribute.getMetadata(), "");
                        String aliasValue = typeRegistry.getAlias(attribute.getValue()) + ONE_BYTE +
                            typeRegistry.encode(attribute.getValue());

                        ColumnVisibility columnVisibility = new ColumnVisibility(visibility);

                        Map<String,String> meta = new HashMap<String, String>(attribute.getMetadata());
                        meta.remove(Metadata.Visiblity.VISIBILITY);

                        Long expiration = Metadata.Expiration.getExpiration(meta, -1);
                        shardVal.set(metadataSerDe.serialize(meta));
                        fiVal.set(expiration.toString().getBytes());

                        if(expiration > -1)
                            minExpiration = min(minExpiration, expiration);

                        forwardCF.set(expiration.toString());  // no need to copy the id when this is going to be rolled up anyways
                        forwardCQ.set(attribute.getKey() + NULL_BYTE + aliasValue);
                        fieldIndexCF.set(PREFIX_FI + NULL_BYTE + buildAttributeKey(item, attribute.getKey()));
                        fieldIndexCQ.set(aliasValue + NULL_BYTE + id);

                        long timestamp  = buildAttributeTimestampForEntity(item);

                        Key key = new Key(new Text(shardId), forwardCF, forwardCQ, columnVisibility, timestamp);
                        Value valuePart = new Value(shardVal);
                        visToKeyCache.put(columnVisibility, Maps.immutableEntry(key, valuePart));

                        shardMutation.put(fieldIndexCF,
                            fieldIndexCQ,
                            columnVisibility,
                            timestamp,
                            fiVal);
                    }

                  for(ColumnVisibility colVis : visToKeyCache.keySet()) {
                    Collection<Map.Entry<Key,Value>> keysValuesToEncode = visToKeyCache.get(colVis);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dout = new DataOutputStream(baos);
                    dout.writeInt(keysValuesToEncode.size());
                    long expirationToWrite = minExpiration == Long.MAX_VALUE ? -1 : minExpiration;
                    dout.writeLong(expirationToWrite);   // -1 means don't expire.
                    encodeRowSimple(keysValuesToEncode, baos);
                    shardMutation.put(new Text(id), new Text(), colVis, buildAttributeTimestampForEntity(item), new Value(baos.toByteArray()));
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


        QueryPlanner queryPlan = new QueryPlanner(query, globalIndexVisitor, typeRegistry);

        if (isEmpty(queryPlan.getOptimizedQuery()))
            return wrap(EMPTY_LIST);

        String jexl = nodeToJexl.transform(types, queryPlan.getOptimizedQuery());
        String originalJexl = nodeToJexl.transform(types, query);
        Set<String> shards = queryPlan.getShards();

        Collection<Range> ranges = new HashSet<Range>();
        // If we were able to determine from the index table that we don't have a match, let's just scan beyond the range of the table
        if(jexl.equals("()") || jexl.equals("") || shards.size() == 0)
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

    protected abstract String buildAttributeKey(T item, String key);

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

    public KeyValueIndex<T> getKeyValueIndex() {
        return keyValueIndex;
    }

    protected abstract long buildAttributeTimestampForEntity(T e);
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
