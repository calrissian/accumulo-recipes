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
import com.google.common.collect.AbstractIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.rangestore.RangeStore;
import org.calrissian.accumulorecipes.rangestore.helper.RangeHelper;
import org.calrissian.mango.domain.ValueRange;
import org.calrissian.mango.types.exception.TypeNormalizationException;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.*;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.data.Range.prefix;

public class AccumuloRangeStore<T extends Comparable<T>> implements RangeStore<T> {

    private static final String LOWER_BOUND_INDEX = "l";
    private static final String UPPER_BOUND_INDEX = "u";
    private static final String DISTANCE_INDEX = "d";

    private static final String DELIM = "\u0000";

    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;
    private final RangeHelper<T> helper;

    public AccumuloRangeStore(Connector connector, RangeHelper<T> helper) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        this(connector, "ranges", helper);
    }

    public AccumuloRangeStore(Connector connector, String tableName, RangeHelper<T> helper) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector, "Invalid connector");
        checkNotNull(tableName, "The table name must not be empty");

        this.connector = connector;
        this.tableName = tableName;
        this.helper = helper;

        if(!connector.tableOperations().exists(this.tableName)) {
            connector.tableOperations().create(this.tableName);
            configureTable(connector, this.tableName);
        }

        writer = connector.createBatchWriter(this.tableName, 10000L, 10000L, 10);
    }

    /**
     * Utility method to update the correct iterators to the table.
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

                Mutation forwardRange = new Mutation(LOWER_BOUND_INDEX + DELIM + low + DELIM + high);
                forwardRange.put(new Text(""), new Text(""), new Value("".getBytes()));

                Mutation reverseRange = new Mutation(UPPER_BOUND_INDEX + DELIM + high + DELIM + low);
                reverseRange.put(new Text(""), new Text(""), new Value("".getBytes()));

                String distanceComplement = helper.encodeComplement(helper.distance(range));
                Mutation distanceMut = new Mutation(DISTANCE_INDEX + DELIM + distanceComplement);
                distanceMut.put(new Text(low), new Text(high), new Value("".getBytes()));

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

                Mutation forwardRange = new Mutation(LOWER_BOUND_INDEX + DELIM + low + DELIM + high);
                forwardRange.putDelete(new Text(""), new Text(""));

                Mutation reverseRange = new Mutation(UPPER_BOUND_INDEX + DELIM + high + DELIM + low);
                reverseRange.putDelete(new Text(""), new Text(""));

                String distanceComplement = helper.encodeComplement(helper.distance(range));
                Mutation distanceMut = new Mutation(DISTANCE_INDEX + DELIM + distanceComplement);
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

    private T getMaxDistance(Authorizations auths) throws TableNotFoundException, TypeNormalizationException {
        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(prefix(DISTANCE_INDEX));
        scanner.setBatchSize(1);

        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        if (!iterator.hasNext())
            return null;

        //Only need the top one, as it should be sorted by size.
        Entry<Key,Value> entry = iterator.next();
        return helper.decodeComplement(entry.getKey().getRow().toString().split(DELIM)[1]);
    }

    /**
     * This will only get ranges who's low value is within the query range.
     */
    private Iterator<ValueRange<T>> forwardIterator(final ValueRange<T> queryRange, Authorizations auths) throws TableNotFoundException, TypeNormalizationException {

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                LOWER_BOUND_INDEX + DELIM + helper.encode(queryRange.getStart()) + DELIM,
                LOWER_BOUND_INDEX + DELIM + helper.encode(queryRange.getStop()) + DELIM + "\uffff"
        ));

        return transform(
                scanner.iterator(),
                new Function<Entry<Key, Value>, ValueRange<T>>() {
                    @Override
                    public ValueRange<T> apply(Entry<Key, Value> entry) {
                        String vals[] = entry.getKey().getRow().toString().split(DELIM);
                        T lower = helper.decode(vals[1]);
                        T upper = helper.decode(vals[2]);
                        return new ValueRange<T>(lower, upper);
                    }
                });
    }

    /**
     * This will only get ranges who's high value is within the query range, ignoring ranges that are fully contained in the query range.
     */
    private Iterator<ValueRange<T>> reverseIterator(final ValueRange<T> queryRange, Authorizations auths) throws TableNotFoundException, TypeNormalizationException {

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                UPPER_BOUND_INDEX + DELIM + helper.encode(queryRange.getStart()) + DELIM,
                UPPER_BOUND_INDEX + DELIM + helper.encode(queryRange.getStop()) + DELIM + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the low values exceed the high range mark.
        return new AbstractIterator<ValueRange<T>>() {
            @Override
            protected ValueRange<T> computeNext() {
                while (iterator.hasNext()) {

                    String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                    T lower = helper.decode(vals[2]);
                    T upper = helper.decode(vals[1]);

                    //If the lower is greater than the query range then it was already picked up in the forward
                    //iterator so ignore it.
                    if (lower.compareTo(queryRange.getStart()) < 0)
                        return new ValueRange<T> (lower, upper);

                    //System.out.println("Wasted reverse");
                }
                return endOfData();
            }
        };
    }

    /**
     * This iterator is looking through all the ranges surrounding the query range to see if it is inside.
     * It uses the knowledge of the max range to limit the query to the ranges closes to the range using the following
     * logic to generate an efficient scan
     *
     * [(high - maxdistance) -> low]
     *
     * It will then ignore all ranges that don't overlap the query range.
     */
    private Iterator<ValueRange<T>> monsterIterator(final ValueRange<T> queryRange, Authorizations auths) throws TableNotFoundException, TypeNormalizationException {

        T maxDistance = getMaxDistance(auths);

        //No point of running this check if the query range has spanned the max distance.
        if (maxDistance == null || helper.distance(queryRange).compareTo(maxDistance) >= 0)
            return emptyIterator();

        //We do [(high - maxdistance) -> low] on the forward table as it would only
        //account for large intervals that are close but outside the range of the ranges already scanned.
        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                LOWER_BOUND_INDEX + DELIM + helper.encode(helper.distance(new ValueRange<T>(maxDistance, queryRange.getStop()))) + DELIM, true,
                LOWER_BOUND_INDEX + DELIM + helper.encode(queryRange.getStart()) + DELIM, false
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and exhaust while trying to find a possible range that intersects.
        return new AbstractIterator<ValueRange<T>>() {
            @Override
            protected ValueRange<T> computeNext() {

                while (iterator.hasNext()) {
                    String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                    T lower = helper.decode(vals[1]);
                    T upper = helper.decode(vals[2]);

                    //Only include ranges where the upper range is higher than query upper range.
                    //as the other ranges were already accounted for in the forward and reverse iterators.
                    if (upper.compareTo(queryRange.getStop()) > 0)
                        return new ValueRange<T>(lower, upper);

                    //System.out.println("Wasted monster");
                }
                return endOfData();
            }
        };
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
        return new Iterable<ValueRange<T>>() {
            @Override
            public Iterator<ValueRange<T>> iterator() {
                try {
                    return concat(
                            forwardIterator(range, authorizations),
                            reverseIterator(range, authorizations),
                            monsterIterator(range, authorizations)
                    );
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
