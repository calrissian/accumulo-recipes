package org.calrissian.accumlorecipes.changelog.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.ChangelogStore;
import org.calrissian.accumlorecipes.changelog.support.Utils;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Collection;
import java.util.Date;

import static java.util.concurrent.TimeUnit.MINUTES;

public class AccumuloChangelogStore implements ChangelogStore{

    protected Long maxMemory = 100000L;
    protected Integer numThreads = 3;
    protected Long maxLatency = 10000L;

    protected String tableName = "changelog";
    protected Connector connector;
    protected BatchWriter writer;

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    public AccumuloChangelogStore(Connector connector) {
        this.connector = connector;

        try {
            createTable();
            writer = connector.createBatchWriter(tableName, maxMemory, maxLatency, numThreads);
        }

        catch(Exception e) {
            throw new RuntimeException("Failed to create changelog table");
        }
    }

    private void createTable() throws TableExistsException, AccumuloException, AccumuloSecurityException {

        if(!connector.tableOperations().exists(tableName)) {
            connector.tableOperations().create(tableName);
        }
    }

    @Override
    public void put(Collection<StoreEntry> changes) {

        for(StoreEntry change : changes) {

            Mutation m = new Mutation(Long.toString(Utils.truncatedReverseTimestamp(change.getTimestamp(), MINUTES)));
            try {
                Text reverseTimestamp = new Text(Long.toString(Utils.reverseTimestamp(change.getTimestamp())));

                System.out.println(reverseTimestamp);
                m.put(reverseTimestamp, new Text(change.getId()), change.getTimestamp(),
                        new Value(objectMapper.writeValueAsBytes(change)));
                writer.addMutation(m);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }

        try {
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public MerkleTree getChangeTree(Date start, Date stop) {
        return null;
    }

    @Override
    public CloseableIterable<StoreEntry> getChanges(Collection<Date> buckets) {
        return null;
    }

    public Long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(Long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public Integer getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(Integer numThreads) {
        this.numThreads = numThreads;
    }

    public Long getMaxLatency() {
        return maxLatency;
    }

    public void setMaxLatency(Long maxLatency) {
        this.maxLatency = maxLatency;
    }

    public String getTableName() {
        return tableName;
    }
}
