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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.calrissian.accumlorecipes.changelog.ChangelogStore;
import org.calrissian.accumlorecipes.changelog.domain.BucketHashLeaf;
import org.calrissian.accumlorecipes.changelog.iterator.BucketHashIterator;
import org.calrissian.accumlorecipes.changelog.support.BucketSize;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Event;
import org.calrissian.mango.hash.tree.MerkleTree;
import org.calrissian.mango.json.tuple.TupleModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Map.Entry;
import static org.calrissian.accumlorecipes.changelog.support.BucketSize.FIVE_MINS;
import static org.calrissian.accumlorecipes.changelog.support.Utils.*;
import static org.calrissian.accumulorecipes.commons.support.Scanners.closeableIterable;
import static org.calrissian.accumulorecipes.commons.support.WritableUtils2.asWritable;
import static org.calrissian.accumulorecipes.commons.support.WritableUtils2.serialize;
import static org.calrissian.mango.collect.CloseableIterables.transform;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

/**
 * An Accumulo implementation of a bucketed merkle tree-based changelog store providing tools to keep data consistent
 * between different connected multicloud environments.
 */
public class AccumuloChangelogStore implements ChangelogStore {

    private static final String DEFAULT_TABLE_NAME = "changelog";
    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(3, 100000L, 10000L, 3);

    private final ObjectMapper objectMapper;

    private final String tableName;
    private final Connector connector;
    private final StoreConfig config;
    private final BucketSize bucketSize;
    private final BatchWriter writer;

    private final Function<Entry<Key, Value>, Event> entityTransform = new Function<Entry<Key, Value>, Event>() {
        @Override
        public Event apply(Entry<Key, Value> entry) {
            try {
                return asWritable(entry.getValue().get(), EventWritable.class).get();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public AccumuloChangelogStore(Connector connector) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        this(connector, FIVE_MINS); // default to a medium sized bucket
    }

    public AccumuloChangelogStore(Connector connector, String tableName, StoreConfig config) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        this(connector, tableName, config, FIVE_MINS); // default to a medium sized bucket
    }

    public AccumuloChangelogStore(Connector connector, BucketSize bucketSize) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG, bucketSize);
    }

    public AccumuloChangelogStore(Connector connector, String tableName, StoreConfig config, BucketSize bucketSize) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector);
        checkNotNull(tableName);
        checkNotNull(config);
        checkNotNull(bucketSize);

        this.connector = connector;
        this.tableName = tableName;
        this.config = config;
        this.bucketSize = bucketSize;
        this.objectMapper = new ObjectMapper().registerModule(new TupleModule(LEXI_TYPES)); //TODO allow caller to pass in types.

        if (!connector.tableOperations().exists(tableName)) {
            connector.tableOperations().create(tableName);
            configureTable(connector, tableName);
        }

        writer = connector.createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
    }

    /**
     * Utility method to update the correct iterators to the table.
     *
     * @param connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void configureTable(Connector connector, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        //Nothing to do for default implementation
    }

    /**
     * Puts a set of changes into buckets of the store.
     *
     * @param changes
     */
    @Override
    public void put(Iterable<Event> changes) {

        EventWritable shared = new EventWritable();
        checkNotNull(changes);
        try {
            for (Event change : changes) {

                shared.set(change);
                Mutation m = new Mutation(Long.toString(truncatedReverseTimestamp(change.getTimestamp(), bucketSize)));
                try {
                    Text reverseTimestamp = new Text(Long.toString(reverseTimestamp(change.getTimestamp())));
                    m.put(reverseTimestamp, new Text(change.getId()), change.getTimestamp(),
                            new Value(serialize(shared)));
                    writer.addMutation(m);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            writer.flush();

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MerkleTree getChangeTree(Date start, Date stop, Auths auths) {
        return getChangeTree(start, stop, 4, auths); //default to a quad tree
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MerkleTree getChangeTree(Date start, Date stop, int dimensions, Auths auths) {
        checkNotNull(start);
        checkNotNull(stop);
        checkArgument(dimensions > 1);
        checkNotNull(auths);

        try {
            Scanner scanner = connector.createScanner(tableName, auths.getAuths());
            IteratorSetting is = new IteratorSetting(2, BucketHashIterator.class);
            BucketHashIterator.setBucketSize(is, bucketSize);
            scanner.addScanIterator(is);

            String startRange = truncatedReverseTimestamp(start.getTime(), bucketSize).toString();
            String endRange = truncatedReverseTimestamp(stop.getTime(), bucketSize).toString();

            scanner.setRange(new Range(endRange, startRange));

            List<BucketHashLeaf> leafList = new ArrayList<BucketHashLeaf>();
            Long prevTs = reverseTimestampToNormalTime(Long.parseLong(endRange));

            int count = 0;
            for (Entry<Key, Value> entry : scanner) {
                Long ts = reverseTimestampToNormalTime(Long.parseLong(entry.getKey().getRow().toString()));


                if (count == 0 && (prevTs - ts > bucketSize.getMs() || ts > prevTs))
                    leafList.add(new BucketHashLeaf("", prevTs));

                /**
                 * It's a little ridiculous that a merkle tree has to guarantee the same number of leaves.
                 * The following while() loop is a padding to make sure we didn't skip any buckets.
                 */
                while (prevTs - ts > bucketSize.getMs()) {

                    leafList.add(new BucketHashLeaf("", prevTs - bucketSize.getMs()));
                    prevTs -= bucketSize.getMs();
                }

                leafList.add(new BucketHashLeaf(new String(entry.getValue().get()), ts));
                prevTs = ts;
                count++;
            }

            Long startTs = reverseTimestampToNormalTime(Long.parseLong(startRange));

            /**
             * If we didn't have a single bucket returned from the Scanner, we need to prime the leaves.
             */
            if (count == 0)
                leafList.add(new BucketHashLeaf("", prevTs));

            while (prevTs - startTs >= bucketSize.getMs()) {
                leafList.add(new BucketHashLeaf("", prevTs - bucketSize.getMs()));
                prevTs -= bucketSize.getMs();
            }

            return new MerkleTree(leafList, dimensions);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the actual change objects that live inside of the specified buckets
     *
     * @param buckets dates representing time increments (i.e. 15 minutes)
     * @return
     */
    @Override
    public CloseableIterable<Event> getChanges(Iterable<Date> buckets, Auths auths) {
        checkNotNull(buckets);
        checkNotNull(auths);
        try {
            final BatchScanner scanner = connector.createBatchScanner(tableName, auths.getAuths(), config.getMaxQueryThreads());

            List<Range> ranges = new ArrayList<Range>();
            for (Date date : buckets) {

                Range range = new Range(String.format("%d", truncatedReverseTimestamp(date.getTime(), bucketSize)));
                ranges.add(range);
            }

            scanner.setRanges(ranges);

            return transform(closeableIterable(scanner), entityTransform);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
