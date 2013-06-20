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

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.iterator.EventIterator;
import org.calrissian.accumulorecipes.eventstore.support.QueryNodeHelper;
import org.calrissian.accumulorecipes.eventstore.support.Shard;
import org.calrissian.accumulorecipes.eventstore.support.query.QueryResultsVisitor;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.serialization.ObjectMapperContext;
import org.calrissian.mango.types.TypeContext;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;

/**
 * The Accumulo implementation of the EventStore which uses deterministic sharding to distribute ingest/queries over
 * the cloud to speed them up.
 */
public class AccumuloEventStore implements EventStore {

    private static final TypeContext typeContext = TypeContext.getInstance();
    private static final Shard shard = new Shard(DEFAULT_PARTITION_SIZE);

    private final  Connector connector;
    private final String indexTable;
    private final String shardTable;
    private final MultiTableBatchWriter multiTableWriter;

    private final QueryNodeHelper queryHelper;

    public AccumuloEventStore(Connector connector) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this(connector, "eventStore_index", "eventStore_shard");
    }

    public AccumuloEventStore(Connector connector, String indexTable, String shardTable) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this.connector = connector;
        this.indexTable = indexTable;
        this.shardTable = shardTable;

        if(!connector.tableOperations().exists(this.indexTable)) {
            connector.tableOperations().create(this.indexTable);
            configureIndexTable(connector, this.indexTable);
        }
        if(!connector.tableOperations().exists(this.shardTable)) {
            connector.tableOperations().create(this.shardTable);
            configureShardTable(connector, this.indexTable);
        }

        this.queryHelper = new QueryNodeHelper(connector, this.shardTable, 3, shard);
        this.multiTableWriter = connector.createMultiTableBatchWriter(100000L, 10000L, 3);
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
     * Utility method to update the correct iterators to the shard table.
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
     * Events get save into a sharded table to parallelize queries & ingest. Since the data is temporal by default,
     * an index table allows the lookup of events by UUID only (when the event's timestamp is not known).
     * @param events
     * @throws Exception
     */
    @Override
    public void save(Iterable<StoreEntry> events) {
        try {
            for(StoreEntry event : events) {

                //If there are no tuples then don't write anything to the data store.
                if(event.getTuples() != null && !event.getTuples().isEmpty()) {

                    String shardId = shard.buildShard(event.getTimestamp(), event.getId());

                    Mutation indexMutation = new Mutation(event.getId());
                    indexMutation.put(new Text(shardId), new Text(""), event.getTimestamp(), new Value("".getBytes()));

                    Mutation shardMutation = new Mutation(shardId);

                    for(Tuple tuple : event.getTuples()) {

                        // forward mutation
                        shardMutation.put(new Text(SHARD_PREFIX_F + DELIM + event.getId()),
                                new Text(tuple.getKey() + DELIM + typeContext.getAliasForType(tuple.getValue()) + DELIM +
                                        typeContext.normalize(tuple.getValue())),
                                new ColumnVisibility(tuple.getVisibility()),
                                event.getTimestamp(),
                                new Value("".getBytes()));

                        // reverse mutation
                        shardMutation.put(new Text(SHARD_PREFIX_B + DELIM + tuple.getKey() + DELIM +
                                typeContext.getAliasForType(tuple.getValue()) + DELIM +
                                typeContext.normalize(tuple.getValue())),
                                new Text(event.getId()),
                                new ColumnVisibility(tuple.getVisibility()),
                                event.getTimestamp(),
                                new Value("".getBytes()));  // forward mutation

                        // value mutation
                        shardMutation.put(new Text(SHARD_PREFIX_V + DELIM + typeContext.getAliasForType(tuple.getValue()) +
                                DELIM + typeContext.normalize(tuple.getValue())),
                                new Text(tuple.getKey() + DELIM + event.getId()),
                                new ColumnVisibility(tuple.getVisibility()),
                                event.getTimestamp(),
                                new Value("".getBytes()));  // forward mutation
                    }

                    multiTableWriter.getBatchWriter(indexTable).addMutation(indexMutation);
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
    public CloseableIterable<StoreEntry> query(Date start, Date end, Node node, Authorizations auths) {
        return new QueryResultsVisitor(node, queryHelper, start, end, auths).getResults();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StoreEntry get(String uuid, Authorizations auths) {

        try {

            Scanner scanner = connector.createScanner(indexTable, auths);
            scanner.setRange(new Range(uuid, uuid + DELIM_END));

            Iterator<Map.Entry<Key,Value>> itr = scanner.iterator();

            if(itr.hasNext()) {

                Map.Entry<Key,Value> entry = itr.next();
                String shardId = entry.getKey().getColumnFamily().toString();

                Scanner eventScanner = connector.createScanner(shardTable, auths);
                eventScanner.setRange(new Range(shardId));
                eventScanner.fetchColumnFamily(new Text(SHARD_PREFIX_F + DELIM + uuid));

                IteratorSetting iteratorSetting = new IteratorSetting(16, "eventIterator", EventIterator.class);
                eventScanner.addScanIterator(iteratorSetting);

                itr = eventScanner.iterator();

                if(itr.hasNext()) {
                    Map.Entry<Key,Value> event = itr.next();

                    return ObjectMapperContext.getInstance().getObjectMapper()
                            .readValue(new String(event.getValue().get()), StoreEntry.class);
                }

            }

            return null;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
