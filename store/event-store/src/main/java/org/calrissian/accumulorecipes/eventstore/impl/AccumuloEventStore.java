package org.calrissian.accumulorecipes.eventstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.accumulorecipes.eventstore.support.Constants;
import org.calrissian.accumulorecipes.eventstore.support.QueryNodeHelper;
import org.calrissian.accumulorecipes.eventstore.support.Shard;
import org.calrissian.accumulorecipes.eventstore.support.query.QueryResultsVisitor;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.criteria.domain.Node;
import org.calrissian.mango.collect.CloseableIterator;
import org.calrissian.mango.types.TypeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.*;

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

    @Override
    public void put(Collection<Event> events) throws Exception {

        for(Event event : events) {

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
    public CloseableIterator<Event> query(Date start, Date end, Node node, Authorizations auths) {
        return new QueryResultsVisitor(node, queryHelper, start, end, auths).getResults();
    }

    @Override
    public Event get(String uuid, Authorizations auths) {
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
