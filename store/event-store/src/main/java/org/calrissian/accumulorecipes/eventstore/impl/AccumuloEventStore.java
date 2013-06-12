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
import org.calrissian.accumulorecipes.eventstore.support.Constants;
import org.calrissian.accumulorecipes.eventstore.support.QueryNodeHelper;
import org.calrissian.accumulorecipes.eventstore.support.Shard;
import org.calrissian.accumulorecipes.eventstore.support.query.QueryResultsVisitor;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.criteria.domain.Node;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.types.TypeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;

/**
 * The Accumulo implementation of the EventStore which uses deterministic sharding to distribute ingest/queries over
 * the cloud to speed them up.
 */
public class AccumuloEventStore implements EventStore {

    Logger logger = LoggerFactory.getLogger(AccumuloEventStore.class);

    protected Connector connector;

    protected BatchWriter shardWriter;
    protected BatchWriter indexWriter;

    protected Long maxMemory = 100000L;
    protected Integer numThreads = 3;
    protected Long maxLatency = 10000L;

    protected String indexTable = "eventStore_index";
    protected String shardTable = "eventStore_shard";

    protected QueryNodeHelper queryHelper;

    protected final Shard shard = new Shard(Constants.DEFAULT_PARTITION_SIZE);

    protected final TypeContext typeContext = TypeContext.getInstance();

    public AccumuloEventStore(Connector connector) {
        this.connector = connector;
        this.queryHelper = new QueryNodeHelper(connector, shardTable, numThreads, shard);

        try {
            initialize();
        } catch (Exception e) {
            logger.error("There was an error initializing the event store. excpetion=" + e);
        }
    }

    /**
     * Create tables if they don't exist and create the batch writers which will be used for the entire instance.
     * @throws TableExistsException
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableNotFoundException
     */
    protected void initialize() throws TableExistsException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if(!connector.tableOperations().exists(indexTable)) {
            connector.tableOperations().create(indexTable);
        }
        if(!connector.tableOperations().exists(shardTable)) {
            connector.tableOperations().create(shardTable);
        }

        indexWriter = connector.createBatchWriter(indexTable, maxMemory, maxLatency, numThreads);
        shardWriter = connector.createBatchWriter(shardTable, maxMemory, maxLatency, numThreads);
    }

    /**
     * Events get put into a sharded table to parallelize queries & ingest. Since the data is temporal by default,
     * an index table allows the lookup of events by UUID only (when the event's timestamp is not known).
     * @param events
     * @throws Exception
     */
    @Override
    public void put(Collection<StoreEntry> events) throws Exception {

        for(StoreEntry event : events) {

            String shardId = shard.buildShard(event.getTimestamp(), event.getId());
            Mutation shardMutation = new Mutation(shardId);

            if(event.getTuples() != null) {

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

                shardWriter.addMutation(shardMutation);
            }

            Mutation indexMutation = new Mutation(event.getId());
            indexMutation.put(new Text(shardId), new Text(""), event.getTimestamp(), new Value("".getBytes()));

            indexWriter.addMutation(indexMutation);
        }

        shardWriter.flush();
        indexWriter.flush();
    }

    @Override
    public CloseableIterable<StoreEntry> query(Date start, Date end, Node node, Authorizations auths) {
        return new QueryResultsVisitor(node, queryHelper, start, end, auths).getResults();
    }

    @Override
    public StoreEntry get(String uuid, Authorizations auths) {

        Scanner scanner = null;
        try {

            scanner = connector.createScanner(indexTable, auths);
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

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    @Override
    public void shutdown() throws Exception {

        shardWriter.close();
        indexWriter.close();
    }

    public void setMaxMemory(Long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public void setNumThreads(Integer numThreads) {
        this.numThreads = numThreads;
    }

    public void setMaxLatency(Long maxLatency) {
        this.maxLatency = maxLatency;
    }

    public void setIndexTable(String indexTable) {
        this.indexTable = indexTable;
    }

    public void setShardTable(String shardTable) {
        this.shardTable = shardTable;
    }

    public Long getMaxMemory() {
        return maxMemory;
    }

    public Integer getNumThreads() {
        return numThreads;
    }

    public Long getMaxLatency() {
        return maxLatency;
    }

    public String getIndexTable() {
        return indexTable;
    }

    public String getShardTable() {
        return shardTable;
    }

    public Shard getShard() {
        return shard;
    }
}
