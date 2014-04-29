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
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.commons.iterators.BooleanLogicIterator;
import org.calrissian.accumulorecipes.commons.iterators.OptimizedQueryIterator;
import org.calrissian.accumulorecipes.commons.iterators.WholeColumnFamilyIterator;
import org.calrissian.accumulorecipes.commons.iterators.support.EventFields;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.support.NodeToJexl;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.calrissian.accumulorecipes.eventstore.support.shard.ShardBuilder;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;
import org.calrissian.mango.types.exception.TypeDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.iterators.support.EventFields.initializeKryo;
import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;
import static org.calrissian.mango.accumulo.Scanners.closeableIterable;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;
import static org.calrissian.mango.collect.CloseableIterables.transform;

/**
 * The Accumulo implementation of the EventStore which uses deterministic sharding to distribute ingest/queries over
 * the cloud to speed them up.
 */
public class AccumuloEventStore implements EventStore {

    private static final String DEFAULT_IDX_TABLE_NAME = "eventStore_index";
    private static final String DEFAULT_SHARD_TABLE_NAME = "eventStore_shard";
    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

    private ShardBuilder shardBuilder = new HourlyShardBuilder(DEFAULT_PARTITION_SIZE);

    private final  Connector connector;
    private final String indexTable;
    private final String shardTable;
    private final MultiTableBatchWriter multiTableWriter;

    private final Kryo kryo = new Kryo();

    private static final NodeToJexl nodeToJexl = new NodeToJexl();

    private static TypeRegistry<String> typeRegistry;

    public AccumuloEventStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, new StoreConfig());
    }

    public AccumuloEventStore(Connector connector, String indexTable, String shardTable, StoreConfig config) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector);
        checkNotNull(indexTable);
        checkNotNull(shardTable);
        checkNotNull(config);

        this.connector = connector;
        this.indexTable = indexTable;
        this.shardTable = shardTable;
        this.typeRegistry = ACCUMULO_TYPES; //TODO allow caller to pass in types.

        if(!connector.tableOperations().exists(this.indexTable)) {
            connector.tableOperations().create(this.indexTable);
            configureIndexTable(connector, this.indexTable);
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
            for(StoreEntry event : events) {

                //If there are no tuples then don't write anything to the data store.
                if(event.getTuples() != null && !event.getTuples().isEmpty()) {

                    String shardId = shardBuilder.buildShard(event.getTimestamp(), event.getId());

                    // key
                    Set<String> indexCache = new HashSet<String>();

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

                        indexCache.add(join(strings, INNER_DELIM));
                    }

                    String[] idIndex = new String[] {
                      shardBuilder.buildShard(event.getTimestamp(), event.getId()),
                      "@id",
                      "string",
                      event.getId(),
                      ""
                    };

                    indexCache.add(join(idIndex, INNER_DELIM));

                    for(String indexCacheKey : indexCache) {

                      String[] indexParts = splitPreserveAllTokens(indexCacheKey, INNER_DELIM);
                      Mutation keyMutation = new Mutation(INDEX_K + "_" + indexParts[1]);
                      Mutation valueMutation = new Mutation(INDEX_V + "_" + indexParts[2] + INNER_DELIM + indexParts[3]);

                      keyMutation.put(new Text(indexParts[2]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), event.getTimestamp(), EMPTY_VALUE);
                      valueMutation.put(new Text(indexParts[1]), new Text(indexParts[0]), new ColumnVisibility(indexParts[4]), event.getTimestamp(), EMPTY_VALUE);
                      multiTableWriter.getBatchWriter(indexTable).addMutation(keyMutation);
                      multiTableWriter.getBatchWriter(indexTable).addMutation(valueMutation);
                    }

                    multiTableWriter.getBatchWriter(shardTable).addMutation(shardMutation);
                }
            }

            multiTableWriter.flush();
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloseableIterable<StoreEntry> query(Date start, final Date end, Node node, Auths auths) {
      String jexl = nodeToJexl.transform(node);
      Set<Text> shards = shardBuilder.buildShardsInRange(start, end);

      try {
        BatchScanner scanner = connector.createBatchScanner(shardTable, auths.getAuths(), DEFAULT_STORE_CONFIG.getMaxQueryThreads());

        Collection<Range> ranges = new HashSet<Range>();
        for(Text shard : shards)
          ranges.add(new Range(shard));

        scanner.setRanges(ranges);

        IteratorSetting setting = new IteratorSetting(16, OptimizedQueryIterator.class);
        setting.addOption(BooleanLogicIterator.QUERY_OPTION, jexl);
        setting.addOption(BooleanLogicIterator.FIELD_INDEX_QUERY, jexl);

        scanner.addScanIterator(setting);

        return transform(closeableIterable(scanner), new Function<Map.Entry<Key, Value>, StoreEntry>() {
          @Override
          public StoreEntry apply(Map.Entry<Key, Value> keyValueEntry) {
            EventFields eventFields = new EventFields();
            eventFields.readObjectData(kryo, ByteBuffer.wrap(keyValueEntry.getValue().get()));
            StoreEntry entry = new StoreEntry(keyValueEntry.getKey().getColumnFamily().toString(), keyValueEntry.getKey().getTimestamp());
            for (Map.Entry<String, EventFields.FieldValue> fieldValue : eventFields.entries()) {
              String[] aliasVal = splitPreserveAllTokens(new String(fieldValue.getValue().getValue()), INNER_DELIM);
              try {
                Object javaVal = typeRegistry.decode(aliasVal[0], aliasVal[1]);
                String vis = fieldValue.getValue().getVisibility().getExpression().length > 0 ? fieldValue.getValue().getVisibility().toString() : "";
                entry.put(new Tuple(fieldValue.getKey(), javaVal, vis));
              } catch (TypeDecodingException e) {
                throw new RuntimeException(e);
              }
            }
            return entry;
          }
        });

      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StoreEntry get(String uuid, Auths auths) {
        checkNotNull(uuid);
        checkNotNull(auths);
        try {

            Scanner scanner = connector.createScanner(indexTable, auths.getAuths());
            scanner.setRange(new Range(INDEX_V + "_string" + INNER_DELIM + uuid));
            scanner.fetchColumnFamily(new Text("@id"));

            Iterator<Map.Entry<Key,Value>> itr = scanner.iterator();

            /**
             * Should just be one index for the shard containing the uuid
             */
            if(itr.hasNext()) {

                Map.Entry<Key,Value> entry = itr.next();
                String shardId = entry.getKey().getColumnQualifier().toString();

                Scanner eventScanner = connector.createScanner(shardTable, auths.getAuths());
                eventScanner.setRange(new Range(shardId));
                eventScanner.fetchColumnFamily(new Text(uuid));

                IteratorSetting iteratorSetting = new IteratorSetting(16, "wholeColumnFamilyIterator", WholeColumnFamilyIterator.class);
                eventScanner.addScanIterator(iteratorSetting);

                itr = eventScanner.iterator();
                if(itr.hasNext())
                  return wholeColFXForm.apply(itr.next());
            }

            return null;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final Function<Map.Entry<Key,Value>, StoreEntry> wholeColFXForm = new Function<Map.Entry<Key, Value>, StoreEntry>() {
      @Override
      public StoreEntry apply(Map.Entry<Key, Value> keyValueEntry) {
        try {
          Map<Key,Value> keyValues = WholeColumnFamilyIterator.decodeRow(keyValueEntry.getKey(), keyValueEntry.getValue());
          StoreEntry entry = null;

          for(Map.Entry<Key,Value> curEntry : keyValues.entrySet()) {
            if(entry == null)
              entry = new StoreEntry(curEntry.getKey().getColumnFamily().toString(), curEntry.getKey().getTimestamp());
            String[] colQParts = splitPreserveAllTokens(curEntry.getKey().getColumnQualifier().toString(), DELIM);
            String[] aliasValue = splitPreserveAllTokens(colQParts[1], INNER_DELIM);
            String visibility = keyValueEntry.getKey().getColumnVisibility().toString();
            try {
              entry.put(new Tuple(colQParts[0], typeRegistry.decode(aliasValue[0], aliasValue[1]), visibility));
            } catch (TypeDecodingException e) {
              throw new RuntimeException(e);
            }
          }

          return entry;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
}
