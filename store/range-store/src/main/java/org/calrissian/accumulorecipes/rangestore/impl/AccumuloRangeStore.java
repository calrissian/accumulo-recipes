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
package org.calrissian.accumulorecipes.rangestore.impl;

import com.google.common.base.Function;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.rangestore.RangeStore;
import org.calrissian.accumulorecipes.rangestore.support.ValueRange;
import org.calrissian.accumulorecipes.rangestore.helper.RangeHelper;
import org.calrissian.accumulorecipes.rangestore.iterator.OverlappingScanFilter;
import org.calrissian.accumulorecipes.rangestore.iterator.ReverseScanFilter;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.emptySet;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.commons.support.Constants.END_BYTE;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;
import static org.calrissian.accumulorecipes.rangestore.support.Constants.DEFAULT_ITERATOR_PRIORITY;

public class AccumuloRangeStore<T extends Comparable<T>> implements RangeStore<T> {

    private static final String DEFAULT_TABLE_NAME = "ranges";
    private static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig(1, 10000L, 10000L, 10);

    private static final String LOWER_BOUND_INDEX = "l";
    private static final String UPPER_BOUND_INDEX = "u";
    private static final String DISTANCE_INDEX = "d";

    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;
    private final RangeHelper<T> helper;

    public AccumuloRangeStore(Connector connector, RangeHelper<T> helper) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG, helper);
    }

    public AccumuloRangeStore(Connector connector, String tableName, StoreConfig config, RangeHelper<T> helper) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector, "Invalid connector");
        checkNotNull(tableName, "The table name must not be empty");
        checkNotNull(config, "Invalid store configuration");

        this.connector = connector;
        this.tableName = tableName;
        this.helper = helper;

        if (!connector.tableOperations().exists(this.tableName)) {
            connector.tableOperations().create(this.tableName);
            configureTable(connector, this.tableName);
        }

        writer = connector.createBatchWriter(this.tableName, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads());
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
        //Nothing special to do for default implementation
    }

    /**
     * Will close all underlying resources
     *
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        writer.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(Iterable<ValueRange<T>> ranges) {
        checkNotNull(ranges);
        try {
            for (ValueRange<T> range : ranges) {

                checkState(helper.isValid(range), "Invalid Range:" + range.toString());

                String low = helper.encode(range.getStart());
                String high = helper.encode(range.getStop());

                Mutation forwardRange = new Mutation(LOWER_BOUND_INDEX + NULL_BYTE + low + NULL_BYTE + high);
                forwardRange.put(new Text(""), new Text(""), new Value("".getBytes()));

                Mutation reverseRange = new Mutation(UPPER_BOUND_INDEX + NULL_BYTE + high + NULL_BYTE + low);
                reverseRange.put(new Text(""), new Text(""), new Value("".getBytes()));

                String distanceComplement = helper.encodeComplement(helper.distance(range));
                Mutation distanceMut = new Mutation(DISTANCE_INDEX + NULL_BYTE + distanceComplement);
                distanceMut.put(new Text(low), new Text(high), new Value("".getBytes()));

                writer.addMutation(forwardRange);
                writer.addMutation(reverseRange);
                writer.addMutation(distanceMut);
            }

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws Exception {
        writer.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(Iterable<ValueRange<T>> ranges) {
        checkNotNull(ranges);

        try {
            for (ValueRange<T> range : ranges) {

                checkState(helper.isValid(range), "Invalid Range:" + range.toString());

                String low = helper.encode(range.getStart());
                String high = helper.encode(range.getStop());

                Mutation forwardRange = new Mutation(LOWER_BOUND_INDEX + NULL_BYTE + low + NULL_BYTE + high);
                forwardRange.putDelete(new Text(""), new Text(""));

                Mutation reverseRange = new Mutation(UPPER_BOUND_INDEX + NULL_BYTE + high + NULL_BYTE + low);
                reverseRange.putDelete(new Text(""), new Text(""));

                String distanceComplement = helper.encodeComplement(helper.distance(range));
                Mutation distanceMut = new Mutation(DISTANCE_INDEX + NULL_BYTE + distanceComplement);
                distanceMut.putDelete(new Text(low), new Text(high));

                writer.addMutation(forwardRange);
                writer.addMutation(reverseRange);
                writer.addMutation(distanceMut);
            }

            writer.flush();

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T getMaxDistance(Authorizations auths) throws TableNotFoundException {
        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(prefix(DISTANCE_INDEX));
        scanner.setBatchSize(1);

        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        if (!iterator.hasNext())
            return null;

        //Only need the top one, as it should be sorted by size.
        Entry<Key, Value> entry = iterator.next();
        return helper.decodeComplement(entry.getKey().getRow().toString().split(NULL_BYTE)[1]);
    }

    /**
     * This will only get ranges who's low value is within the criteria range.
     */
    private Iterable<ValueRange<T>> forwardScan(final ValueRange<T> queryRange, Authorizations auths) throws TableNotFoundException {

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                LOWER_BOUND_INDEX + NULL_BYTE + helper.encode(queryRange.getStart()) + NULL_BYTE,
                LOWER_BOUND_INDEX + NULL_BYTE + helper.encode(queryRange.getStop()) + NULL_BYTE + END_BYTE
        ));

        return transform(scanner, new RangeTransform<T>(helper, true));
    }

    /**
     * This will only get ranges who's high value is within the criteria range, ignoring ranges that are fully contained
     * in the criteria range via the use of a filter.
     */
    private Iterable<ValueRange<T>> reverseScan(final ValueRange<T> queryRange, Authorizations auths) throws TableNotFoundException {

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                UPPER_BOUND_INDEX + NULL_BYTE + helper.encode(queryRange.getStart()) + NULL_BYTE,
                UPPER_BOUND_INDEX + NULL_BYTE + helper.encode(queryRange.getStop()) + NULL_BYTE + END_BYTE
        ));

        //Configure filter to remove any ranges that are fully contained in the criteria range, as they
        //have already been picked up by the forward scan.
        IteratorSetting setting = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, ReverseScanFilter.class);
        ReverseScanFilter.setQueryLowBound(setting, helper.encode(queryRange.getStart()));
        scanner.addScanIterator(setting);

        return transform(scanner, new RangeTransform<T>(helper, false));
    }

    /**
     * This iterator is looking through all the ranges surrounding the criteria range to see if it is inside.
     * It uses the knowledge of the max range to limit the criteria to the ranges closest to the range using the following
     * logic to generate an efficient scan
     * <p/>
     * [(high - maxdistance) -> low]
     * <p/>
     * It will then ignore all ranges that don't overlap the criteria range, via the use of a filter.
     */
    private Iterable<ValueRange<T>> overlappingScan(final ValueRange<T> queryRange, Authorizations auths) throws TableNotFoundException {

        T maxDistance = getMaxDistance(auths);

        //No point of running this check if the criteria range has spanned the max distance.
        if (maxDistance == null || helper.distance(queryRange).compareTo(maxDistance) >= 0)
            return emptySet();

        //We do [(high - maxdistance) -> low] on the forward table as it would only
        //account for large intervals that are close but outside the range of the ranges already scanned.
        final Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                LOWER_BOUND_INDEX + NULL_BYTE + helper.encode(helper.distance(new ValueRange<T>(maxDistance, queryRange.getStop()))) + NULL_BYTE, true,
                LOWER_BOUND_INDEX + NULL_BYTE + helper.encode(queryRange.getStart()) + NULL_BYTE, false
        ));

        //Configure Filter to filter out any ranges that don't overlap the criteria range as they
        //either are outside the criteria range or have already been picked up by the forward and reverse scans.
        IteratorSetting setting = new IteratorSetting(DEFAULT_ITERATOR_PRIORITY, OverlappingScanFilter.class);
        OverlappingScanFilter.setQueryUpperBound(setting, helper.encode(queryRange.getStop()));
        scanner.addScanIterator(setting);

        return transform(scanner, new RangeTransform<T>(helper, true));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<ValueRange<T>> query(final ValueRange<T> range, Auths auths) {
        checkNotNull(range);
        checkNotNull(auths);
        checkState(helper.isValid(range), "Invalid range.");

        final Authorizations authorizations = auths.getAuths();

        try {
            return concat(
                    forwardScan(range, authorizations),
                    reverseScan(range, authorizations),
                    overlappingScan(range, authorizations)
            );

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class RangeTransform<T extends Comparable<T>> implements Function<Entry<Key, Value>, ValueRange<T>> {

        private final RangeHelper<T> helper;
        private final int lowIdx;
        private final int highIdx;

        private RangeTransform(RangeHelper<T> helper, boolean lowFirst) {
            this.helper = helper;
            this.lowIdx = (lowFirst ? 1 : 2);
            this.highIdx = (lowFirst ? 2 : 1);
        }

        @Override
        public ValueRange<T> apply(Entry<Key, Value> entry) {
            String vals[] = splitPreserveAllTokens(entry.getKey().getRow().toString(), NULL_BYTE);
            T lower = helper.decode(vals[lowIdx]);
            T upper = helper.decode(vals[highIdx]);
            return new ValueRange<T>(lower, upper);
        }
    }

    public static class Builder<T extends Comparable<T>> {
        private final Connector connector;
        private String tableName = DEFAULT_TABLE_NAME;
        private StoreConfig config = DEFAULT_STORE_CONFIG;
        private final RangeHelper<T> helper;

        public Builder(Connector connector, RangeHelper<T> helper) {
            checkNotNull(connector);
            checkNotNull(helper);
            this.connector = connector;
            this.helper = helper;
        }

        public Builder setTableName(String tableName) {
            checkNotNull(tableName);
            this.tableName = tableName;
            return this;
        }

        public Builder setConfig(StoreConfig config) {
            checkNotNull(config);
            this.config = config;
            return this;
        }

        public AccumuloRangeStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            return new AccumuloRangeStore<T>(connector, tableName, config, helper);
        }

    }
}
