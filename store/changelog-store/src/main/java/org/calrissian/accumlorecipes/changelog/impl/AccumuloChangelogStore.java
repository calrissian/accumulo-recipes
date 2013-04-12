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
package org.calrissian.accumlorecipes.changelog.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.ChangelogStore;
import org.calrissian.accumlorecipes.changelog.domain.BucketHashLeaf;
import org.calrissian.accumlorecipes.changelog.iterator.BucketHashIterator;
import org.calrissian.accumlorecipes.changelog.support.BucketSize;
import org.calrissian.accumlorecipes.changelog.support.EntryIterator;
import org.calrissian.accumlorecipes.changelog.support.Utils;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.collect.CloseableIterator;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;

/**
 * An Accumulo implementation of a bucketed merkle tree-based changelog store providing tools to keep data consistent
 * between different connected multicloud environments.
 */
public class AccumuloChangelogStore implements ChangelogStore {

    protected Long maxMemory = 100000L;
    protected Integer numThreads = 3;
    protected Long maxLatency = 10000L;

    protected String tableName = "changelog";
    protected Connector connector;
    protected BatchWriter writer;

    protected int merkleAry = 4;    // default the merkle to a quad tree

    protected BucketSize bucketSize = BucketSize.FIVE_MINS; // default to a medium sized bucket

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    public AccumuloChangelogStore(Connector connector) {
        this.connector = connector;

        init();
    }

    public AccumuloChangelogStore(Connector connector, String tableName) {
        this.connector = connector;
        this.tableName = tableName;

        init();
    }

    private void init() {

        try {
            if(!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }

            writer = connector.createBatchWriter(tableName, maxMemory, maxLatency, numThreads);

        } catch(Exception e) {
            throw new RuntimeException("Failed to create changelog table");
        }
    }

    /**
     * Puts a set of changes into buckets of the store.
     * @param changes
     */
    @Override
    public void put(Collection<StoreEntry> changes) {

        for(StoreEntry change : changes) {

            Mutation m = new Mutation(Long.toString(Utils.truncatedReverseTimestamp(change.getTimestamp(), bucketSize)));
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

    /**
     * Returns a Merkle Tree rollup representing the buckets of the given time range.
     * @param start
     * @param stop
     * @return
     */
    @Override
    public MerkleTree getChangeTree(Date start, Date stop) {

        Scanner scanner = null;
        try {
            scanner = connector.createScanner(tableName, new Authorizations());
            IteratorSetting is = new IteratorSetting(2, BucketHashIterator.class);
            scanner.addScanIterator(is);

            String startRange = Utils.truncatedReverseTimestamp(start.getTime(), bucketSize).toString();
            String endRange = Utils.truncatedReverseTimestamp(stop.getTime(), bucketSize).toString();

            scanner.setRange(new Range(endRange, startRange));

            List<BucketHashLeaf> leafList = new ArrayList<BucketHashLeaf>();
            Long prevTs = Utils.reverseTimestampToNormalTime(Long.parseLong(endRange));

            int count = 0;
            for(Map.Entry<Key,Value> entry : scanner) {
                Long ts = Utils.reverseTimestampToNormalTime(Long.parseLong(entry.getKey().getRow().toString()));


                if(count == 0 && (prevTs - ts > bucketSize.getMs() || ts > prevTs)) {
                    leafList.add(new BucketHashLeaf("", prevTs));
                }

                /**
                 * It's a little ridiculous that a merkle tree has to guarantee the same number of leaves.
                 * The following while() loop is a padding to make sure we didn't skip any buckets.
                 */
                while(prevTs - ts > bucketSize.getMs()) {

                    leafList.add(new BucketHashLeaf("", prevTs - bucketSize.getMs()));
                    prevTs -= bucketSize.getMs();
                }

                leafList.add(new BucketHashLeaf(new String(entry.getValue().get()), ts));
                prevTs = ts;
                count++;
            }

            Long startTs = Utils.reverseTimestampToNormalTime(Long.parseLong(startRange));

            /**
             * If we didn't have a single bucket returned from the Scanner, we need to prime the leafs.
             */
            if(count == 0) {
                leafList.add(new BucketHashLeaf("", prevTs));
            }

            while(prevTs - startTs >= bucketSize.getMs()) {
                leafList.add(new BucketHashLeaf("", prevTs - bucketSize.getMs()));
                prevTs -= bucketSize.getMs();
            }

            return new MerkleTree(leafList, merkleAry);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the actual change objects that live inside of the specified buckets
     * @param buckets dates representing time increments (i.e. 15 minutes)
     * @return
     */
    @Override
    public CloseableIterator<StoreEntry> getChanges(Collection<Date> buckets) {

        try {
            final BatchScanner scanner = connector.createBatchScanner(tableName, new Authorizations(), numThreads);

            List<Range> ranges = new ArrayList<Range>();
            for(Date date : buckets) {

                Range range = new Range(String.format("%d", Utils.truncatedReverseTimestamp(date.getTime(), bucketSize)));
                ranges.add(range);
            }

            scanner.setRanges(ranges);

            return new EntryIterator(scanner);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
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

    public void setBucketSize(BucketSize bucketSize) {
        this.bucketSize = bucketSize;
    }

    public BucketSize getBucketSize() {
        return bucketSize;
    }

    public int getMerkleAry() {
        return merkleAry;
    }

    public void setMerkleAry(int merkleAry) {
        this.merkleAry = merkleAry;
    }
}
