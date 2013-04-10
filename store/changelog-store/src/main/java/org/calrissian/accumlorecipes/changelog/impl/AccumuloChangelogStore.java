package org.calrissian.accumlorecipes.changelog.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.ChangelogStore;
import org.calrissian.accumlorecipes.changelog.domain.BucketHashLeaf;
import org.calrissian.accumlorecipes.changelog.iterator.BucketHashIterator;
import org.calrissian.accumlorecipes.changelog.support.Utils;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;

import static java.util.concurrent.TimeUnit.HOURS;

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

            Mutation m = new Mutation(Long.toString(Utils.truncatedReverseTimestamp(change.getTimestamp(), HOURS)));
            try {
                Text reverseTimestamp = new Text(Long.toString(Utils.reverseTimestamp(change.getTimestamp())));
                m.put(reverseTimestamp, new Text(change.getId()), change.getTimestamp(),
                        new Value(objectMapper.writeValueAsBytes(change)));
                writer.addMutation(m);
            } catch (Exception e) {
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

        Scanner scanner = null;
        try {
            scanner = connector.createScanner(tableName, new Authorizations());
            IteratorSetting is = new IteratorSetting(2, BucketHashIterator.class);
            scanner.addScanIterator(is);

            List<BucketHashLeaf> leafList = new ArrayList<BucketHashLeaf>();
            for(Map.Entry<Key,Value> entry : scanner) {

                leafList.add(new BucketHashLeaf(new String(entry.getValue().get()),
                        Utils.reverseTimestampToNormalTime(Long.parseLong(entry.getKey().getRow().toString()))));
            }

            return new MerkleTree(leafList);

        } catch (TableNotFoundException e) {

            throw new RuntimeException(e);
        }
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
