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
package org.calrissian.accumulorecipes.eventstore.impl;

import com.esotericsoftware.kryo.Kryo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.EventFieldsFilteringIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.NodeToJexl;
import org.calrissian.accumulorecipes.commons.support.criteria.QueryOptimizer;
import org.calrissian.accumulorecipes.commons.support.criteria.visitors.GlobalIndexVisitor;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionQueryXform;
import org.calrissian.accumulorecipes.commons.transform.KeyToTupleCollectionWholeColFXform;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.support.EventGlobalIndexVisitor;
import org.calrissian.accumulorecipes.eventstore.support.EventIndex;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.calrissian.accumulorecipes.eventstore.support.shard.ShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.union;
import static java.lang.System.currentTimeMillis;
import static java.util.EnumSet.allOf;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import static org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope.majc;
import static org.apache.commons.lang.StringUtils.*;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.commons.support.Constants.*;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

/**
 * The Accumulo implementation of the EventStore which uses deterministic sharding to distribute ingest/queries over
 * the cloud to speed them up.
 */
public class AccumuloEventStore implements EventStore {

    public static final String DEFAULT_IDX_TABLE_NAME = "eventStore_index";
    public static final String DEFAULT_SHARD_TABLE_NAME = "eventStore_shard";

    public static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

    public static final ShardBuilder DEFAULT_SHARD_BUILDER = new HourlyShardBuilder(DEFAULT_PARTITION_SIZE);

    private ShardBuilder shardBuilder;
    private final  Connector connector;
    private final String indexTable;
    private final String shardTable;
    private final StoreConfig config;
    private final MultiTableBatchWriter multiTableWriter;

    private static final Kryo kryo = new Kryo();

    private final NodeToJexl nodeToJexl;

    private static TypeRegistry<String> typeRegistry;

    public AccumuloEventStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloEventStore(Connector connector, String indexTable, String shardTable, StoreConfig config) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector);
        checkNotNull(indexTable);
        checkNotNull(shardTable);
        checkNotNull(config);

        this.connector = connector;
        this.indexTable = indexTable;
        this.shardTable = shardTable;
        this.typeRegistry = LEXI_TYPES;
        this.config = config;
        this.shardBuilder = DEFAULT_SHARD_BUILDER;

        this.nodeToJexl = new NodeToJexl();

        if(!connector.tableOperations().exists(this.indexTable)) {
            connector.tableOperations().create(this.indexTable);
            configureIndexTable(connector, this.indexTable);
        }

        if(connector.tableOperations().getIteratorSetting(this.indexTable, "cardinalities", majc) == null) {
            IteratorSetting setting = new IteratorSetting(10, "cardinalities", SummingCombiner.class);
            SummingCombiner.setCombineAllColumns(setting, true);
            SummingCombiner.setEncodingType(setting, LongCombiner.StringEncoder.class);
            connector.tableOperations().attachIterator(this.indexTable, setting, allOf(IteratorScope.class));
        }
        if(!connector.tableOperations().exists(this.shardTable)) {
            connector.tableOperations().create(this.shardTable);
            configureShardTable(connector, this.shardTable);
        }

        initializeKryo(kryo);
        this.multiTableWriter = connector.createMultiTableBatchWriter(config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    /**
     * Utility method to update the correct iterators to the index table.
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureIndexTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Do nothing for default implementation
    }

    /**
     * Utility method to update the correct iterators to the shardBuilder table.
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureShardTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Do nothing for default implementation
    }

    /**
     * Free up any resources used by the store.
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        multiTableWriter.close();
    }

    /**
     * A new shard scheme can be plugged in fairly easily to shard to the day, for instance, instead of the hour.
     * The number of partitions can also be changed and the event store provisioned with the new builder.
     * @param shardBuilder
     */
    public void setShardBuilder(ShardBuilder shardBuilder) {
        this.shardBuilder = shardBuilder;
    }

    /**
     * Events get save into a sharded table to parallelize queries & ingest. Since the data is temporal by default,
     * an index table allows the lookup of events by UUID only (when the event's timestamp is not known).
     * @param events
     * @throws Exception
     */
    @Override
    public void save(Iterable<StoreEntry> events) {
        checkNotNull(events);
        try {
            // key
            Map<String, Long> indexCache = new HashMap<String, Long>();

            for(StoreEntry event : events) {

                //If there are no getTuples then don't write anything to the data store.
                if(event.getTuples() != null && !event.getTuples().isEmpty()) {

                    String shardId = shardBuilder.buildShard(event.getTimestamp(), event.getId());

                    Mutation shardMutation = new Mutation(shardId);

                    for(Tuple tuple : event.getTuples()) {

                        String aliasValue = typeRegistry.getAlias(tuple.getValue()) + INNER_DELIM +
                                typeRegistry.encode(tuple.getValue());

                        // forward mutation
                        shardMutation.put(new Text(event.getId()),
                                new Text(tuple.getKey() + DELIM + aliasValue),
                                new ColumnVisibility(tuple.getVisibility()),
                                event.getTimestamp(),
                                EMPTY_VALUE);

                        // reverse mutation
                        shardMutation.put(new Text(PREFIX_FI + DELIM + tuple.getKey()),
                                new Text(aliasValue + DELIM + event.getId()),
                                new ColumnVisibility(tuple.getVisibility()),
                                event.getTimestamp(),
                                EMPTY_VALUE);  // forward mutation

                        String[] strings = new String[] {
                                shardId,
                                tuple.getKey(),
                                typeRegistry.getAlias(tuple.getValue()),
                                typeRegistry.encode(tuple.getValue()),
                                tuple.getVisibility()
                        };

                        String cacheKey = join(strings, INNER_DELIM);
                        Long count = indexCache.get(cacheKey);
                        if(count == null)
                            count = 0l;
                        indexCache.put(cacheKey, ++count);

                    }

                    String[] idIndex = new String[] {
                            shardBuilder.buildShard(event.getTimestamp(), event.getId()),
                            "@id",
                            "string",
                            event.getId(),
                            ""
                    };

                    indexCache.put(join(idIndex, INNER_DELIM), 1l);

                    multiTableWriter.getBatchWriter(shardTable).addMutation(shardMutation);
                }

            }

            for(Map.Entry<String, Long> indexCacheKey : indexCache.entrySet()) {

                String[] indexParts = splitPreserveAllTokens(indexCacheKey.getKey(), INNER_DELIM);
                Mutation keyMutation = new Mutation(INDEX_K + "_" + indexParts[1]);
                Mutation valueMutation = new Mutation(INDEX_V + "_" + indexParts[2] + "__" + indexParts[3]);

                keyMutation.put(new Text(indexParts[2]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), currentTimeMillis(),
                        new Value(Long.toString(indexCacheKey.getValue()).getBytes()));
                valueMutation.put(new Text(indexParts[1]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), currentTimeMillis(),
                        new Value(Long.toString(indexCacheKey.getValue()).getBytes()));
                multiTableWriter.getBatchWriter(indexTable).addMutation(keyMutation);
                multiTableWriter.getBatchWriter(indexTable).addMutation(valueMutation);
            }

            multiTableWriter.flush();

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Shard ids for which to scan are generated from the given start and end time. The given query specifies
     * which events to return. It is propagated all the way down to the iterators so that the query is run in parallel
     * on each tablet that needs to be scanned.
     */
    @Override
    public CloseableIterable<StoreEntry> query(Date start, Date end, Node query, final Set<String> selectFields, Auths auths) {


        try {

            BatchScanner indexScanner = connector.createBatchScanner(indexTable, auths.getAuths(), config.getMaxQueryThreads());
            GlobalIndexVisitor globalIndexVisitor = new EventGlobalIndexVisitor(start, end, indexScanner, shardBuilder);
            QueryOptimizer optimizer = new QueryOptimizer(query, globalIndexVisitor);

            String jexl = nodeToJexl.transform(optimizer.getOptimizedQuery());
            Set<String> shards = optimizer.getShards();


            BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), config.getMaxQueryThreads());

            Collection<Range> ranges = new HashSet<Range>();
            for(String shard : shards)
                ranges.add(new Range(shard));

            scanner.setRanges(ranges);

            IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
            setting.addOption(BooleanLogicIterator.QUERY_OPTION, jexl);
            setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

            scanner.addScanIterator(setting);

            if(selectFields != null && selectFields.size() > 0) {
                setting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
                EventFieldsFilteringIterator.setSelectFields(setting, union(selectFields, optimizer.getKeysInQuery()));
                scanner.addScanIterator(setting);
            }

            return transform(closeableIterable(scanner), new QueryXform(selectFields));

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method will batch get a bunch of events by uuid (and optionally timestamp). If another store is used to
     * index into events in this store in a specially designed way (i.e. getting the last-n events, etc...) then
     * the most optimal way to store the index would be the UUID and the timestamp. However, if all that is known
     * about an event is the uuid, this method will do the extra fetch of the timestamp from the index table.
     */
    @Override
    public CloseableIterable<StoreEntry> get(Collection<EventIndex> uuids, Set<String> selectFields, Auths auths) {
        checkNotNull(uuids);
        checkNotNull(auths);
        try {

            BatchScanner scanner = connector.createBatchScanner(indexTable, auths.getAuths(), DEFAULT_STORE_CONFIG.getMaxQueryThreads());

            Collection<Range> ranges = new LinkedList<Range>();
            for(EventIndex uuid : uuids) {
                if(uuid.getTimestamp() == null)
                    ranges.add(new Range(INDEX_V + "_string__" + uuid.getUuid()));
            }

            scanner.setRanges(ranges);
            scanner.fetchColumnFamily(new Text("@id"));

            Iterator<Map.Entry<Key,Value>> itr = scanner.iterator();

            /**
             * Should just be one index for the shard containing the uuid
             */
            BatchScanner eventScanner = connector.createBatchScanner(shardTable, auths.getAuths(), config.getMaxQueryThreads());
            Collection<Range> eventRanges = new LinkedList<Range>();
            while(itr.hasNext()) {
                Map.Entry<Key,Value> entry = itr.next();
                String shardId = entry.getKey().getColumnQualifier().toString();
                String[] rowParts = splitByWholeSeparatorPreserveAllTokens(entry.getKey().getRow().toString(), "__");
                eventRanges.add(Range.prefix(shardId, rowParts[1]));
            }

            scanner.close();
            for(EventIndex curIndex : uuids) {
                if(curIndex.getTimestamp() != null) {
                    String shardId = shardBuilder.buildShard(curIndex.getTimestamp(), curIndex.getUuid());
                    ranges.add(Range.prefix(shardId, curIndex.getUuid()));
                }
            }

            eventScanner.setRanges(eventRanges);

            IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
            eventScanner.addScanIterator(iteratorSetting);

            if(selectFields != null && selectFields.size() > 0) {
                iteratorSetting = new IteratorSetting(15, EventFieldsFilteringIterator.class);
                EventFieldsFilteringIterator.setSelectFields(iteratorSetting, selectFields);
                eventScanner.addScanIterator(iteratorSetting);
            }

            return transform(closeableIterable(eventScanner), new WholeColFXForm());
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class QueryXform extends KeyToTupleCollectionQueryXform<StoreEntry>{

        public QueryXform(Set<String> selectFields) {
            super(kryo, typeRegistry, selectFields);
        }

        @Override
        protected StoreEntry buildTupleCollectionFromKey(Key k) {
            return new StoreEntry(k.getColumnFamily().toString(), k.getTimestamp());
        }
    }

    public static class WholeColFXForm extends KeyToTupleCollectionWholeColFXform<StoreEntry> {
        public WholeColFXForm() {
            super(kryo, typeRegistry, null);
        }

        @Override
        protected StoreEntry buildEntryFromKey(Key k) {
            return new StoreEntry(k.getColumnFamily().toString(), k.getTimestamp());
        }
    };
}
