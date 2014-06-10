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

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.support.metadata.BaseMetadataSerDe;
import org.calrissian.accumulorecipes.commons.support.metadata.MetadataSerDe;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.criteria.support.NodeUtils;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.TupleStore;
import org.calrissian.mango.types.TypeRegistry;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.EMPTY_LIST;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope.majc;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.VISIBILITY;
import static org.calrissian.accumulorecipes.commons.support.tuple.Metadata.Visiblity.getVisibility;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.collect.CloseableIterables.wrap;

public abstract class QfdHelper<T extends TupleStore> {

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

    private KeyValueIndex<T> keyValueIndex;

    public QfdHelper(Connector connector, String indexTable, String shardTable, StoreConfig config,
                     ShardBuilder<T> shardBuilder, TypeRegistry<String> typeRegistry, KeyValueIndex<T> keyValueIndex)
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
        this.nodeToJexl = new NodeToJexl(typeRegistry);

        this.metadataSerDe = new BaseMetadataSerDe(typeRegistry);

        if (!connector.tableOperations().exists(this.indexTable)) {
            connector.tableOperations().create(this.indexTable);
            configureIndexTable(connector, this.indexTable);
        }

        if (connector.tableOperations().getIteratorSetting(this.indexTable, "cardinalities", majc) == null) {
            IteratorSetting setting = new IteratorSetting(10, "cardinalities", SummingCombiner.class);
            SummingCombiner.setCombineAllColumns(setting, true);
            SummingCombiner.setEncodingType(setting, LongCombiner.StringEncoder.class);
            connector.tableOperations().attachIterator(this.indexTable, setting, allOf(IteratorUtil.IteratorScope.class));
        }

        if (!connector.tableOperations().exists(this.shardTable)) {
            connector.tableOperations().create(this.shardTable);
            configureShardTable(connector, this.shardTable);
        }

        initializeKryo(kryo);
        this.shardWriter = connector.createBatchWriter(shardTable, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    public void setMetadataSerDe(MetadataSerDe metadataSerDe) {
        this.metadataSerDe = metadataSerDe;
    }

    public MetadataSerDe getMetadataSerDe() {
        return metadataSerDe;
    }

    public static Kryo getKryo() {
        return kryo;
    }

    /**
     * Items get saved into a sharded table to parallelize queries & ingest.
     */
    public void save(Iterable<? extends T> items) {
        checkNotNull(items);
        try {

            for (T item : items) {

                //If there are no getTuples then don't write anything to the data store.
                if (item.getTuples() != null && !item.getTuples().isEmpty()) {

                    String shardId = shardBuilder.buildShard(item);

                    Mutation shardMutation = new Mutation(shardId);

                    for (Tuple tuple : item.getTuples()) {

                        String aliasValue = typeRegistry.getAlias(tuple.getValue()) + INNER_DELIM +
                                typeRegistry.encode(tuple.getValue());

                        ColumnVisibility columnVisibility = new ColumnVisibility(getVisibility(tuple, ""));
                        Map<String,Object> metadata = new HashMap<String, Object>(tuple.getMetadata());
                        metadata.remove(VISIBILITY);    // save a little space by removing this.

                        // forward mutation
                        shardMutation.put(new Text(buildId(item)),
                                new Text(tuple.getKey() + DELIM + aliasValue),
                                columnVisibility,
                                buildTimestamp(item),
                                new Value(metadataSerDe.serialize(metadata)));

                        // reverse mutation
                        shardMutation.put(new Text(PREFIX_FI + DELIM + tuple.getKey()),
                                new Text(aliasValue + DELIM + buildId(item)),
                                columnVisibility,
                                buildTimestamp(item),
                                new Value(metadataSerDe.serialize(metadata)));  // forward mutation
                    }

                    shardWriter.addMutation(shardMutation);
                }
            }

            shardWriter.flush();

            keyValueIndex.indexKeyValues(items);
            keyValueIndex.commit();

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CloseableIterable<T> query(BatchScanner scanner, GlobalIndexVisitor globalIndexVisitor, Node query,
                                      Function<Map.Entry<Key, Value>, T> transform, Auths auths) {
        checkNotNull(query);
        checkNotNull(auths);

        QueryOptimizer optimizer = new QueryOptimizer(query, globalIndexVisitor, typeRegistry);

        if (NodeUtils.isEmpty(optimizer.getOptimizedQuery()))
            return wrap(EMPTY_LIST);

        String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());
        String originalJexl = nodeToJexl.transform(query);
        Set<String> shards = optimizer.getShards();

        Collection<Range> ranges = new HashSet<Range>();
        for (String shard : shards)
            ranges.add(new Range(shard));

        scanner.setRanges(ranges);

        IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
        setting.addOption(BooleanLogicIterator.QUERY_OPTION, originalJexl);
        setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

        scanner.addScanIterator(setting);

        return transform(closeableIterable(scanner), transform);
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

    protected abstract Value buildValue(T item);

    protected abstract long buildTimestamp(T item);

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
}
