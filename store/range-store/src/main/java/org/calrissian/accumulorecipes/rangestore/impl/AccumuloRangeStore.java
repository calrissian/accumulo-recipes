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

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.emptyIterator;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.data.Range.prefix;

public class AccumuloRangeStore<T extends Comparable<T>> implements RangeStore<T> {

    private static final String INDEX_FORWARD = "f";
    private static final String INDEX_REVERSE = "r";
    private static final String INDEX_MAXDISTANCE = "s";

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

                String lowComplement = helper.encodeComplement(range.getStart());
                String low = helper.encode(range.getStart());

                String highComplement = helper.encodeComplement(range.getStop());
                String high = helper.encode(range.getStop());

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.put(new Text(""), new Text(""), new Value("".getBytes()));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.put(new Text(""), new Text(""), new Value("".getBytes()));

                String distanceComplement = helper.encodeComplement(helper.distance(range));
                Mutation maxSize = new Mutation(INDEX_MAXDISTANCE + DELIM + distanceComplement);
                maxSize.put(new Text(low), new Text(high), new Value("".getBytes()));

                writer.addMutation(forward);
                writer.addMutation(reverse);
                writer.addMutation(maxSize);
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

                String lowComplement = helper.encodeComplement(range.getStart());
                String low = helper.encode(range.getStart());

                String highComplement = helper.encodeComplement(range.getStop());
                String high = helper.encode(range.getStop());

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.putDelete(new Text(""), new Text(""));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.putDelete(new Text(""), new Text(""));

                String distanceComplement = helper.encodeComplement(helper.distance(range));
                Mutation maxSize = new Mutation(INDEX_MAXDISTANCE + DELIM + distanceComplement);
                maxSize.putDelete(new Text(low), new Text(high));

                writer.addMutation(forward);
                writer.addMutation(reverse);
                writer.addMutation(maxSize);
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
        scanner.setRange(prefix(INDEX_MAXDISTANCE));
        scanner.setBatchSize(1);

        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        if (!iterator.hasNext())
            return null;

        //Only need the top one, as it should be sorted by size.
        Entry<Key,Value> entry = iterator.next();
        return helper.decodeComplement(entry.getKey().getRow().toString().split(DELIM)[1]);
    }

    private Iterator<ValueRange<T>> forwardIterator(final T rangeLow, final T rangeHigh, Authorizations auths, final ValueRange<T> extremes) throws TableNotFoundException {

        String lowComplement = helper.encodeComplement(rangeLow);
        String high = helper.encode(rangeHigh);

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_FORWARD + DELIM + high + DELIM + lowComplement, false,
                INDEX_FORWARD + "\uffff", true
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the low values exceed the high range mark.
        return new AbstractIterator<ValueRange<T>>() {
            @Override
            protected ValueRange<T> computeNext() {
                if (!iterator.hasNext())
                    return endOfData();

                String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                final T upper = helper.decode(vals[1]);
                final T lower = helper.decodeComplement(vals[2]);

                //If we have gone past the high value then stop iterating
                if(lower.compareTo(rangeHigh) > 0)
                    return endOfData();

                if (upper.compareTo(extremes.getStop()) > 0)
                    extremes.setStop(upper);

                if (lower.compareTo(extremes.getStart()) < 0)
                    extremes.setStart(lower);

                return new ValueRange<T> (lower, upper);
            }
        };
    }

    private Iterator<ValueRange<T>> reverseIterator(final T rangeLow, final T rangeHigh, Authorizations auths, final ValueRange<T> extremes) throws TableNotFoundException {

        String low = helper.encode(rangeLow);
        String highComplement = helper.encodeComplement(rangeHigh);

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_REVERSE + DELIM + highComplement + DELIM + low,
                INDEX_REVERSE + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the high values fall below the low range mark.
        return new AbstractIterator<ValueRange<T>>() {
            @Override
            protected ValueRange<T> computeNext() {
                if (!iterator.hasNext())
                    return endOfData();

                String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                final T upper = helper.decodeComplement(vals[1]);
                final T lower = helper.decode(vals[2]);

                //If we have gone past the low value then stop iterating
                if(upper.compareTo(rangeLow) < 0)
                    return endOfData();

                if (upper.compareTo(extremes.getStop()) > 0)
                    extremes.setStop(upper);

                if (lower.compareTo(extremes.getStart()) < 0)
                    extremes.setStart(lower);

                return new ValueRange<T>(lower, upper);
            }
        };
    }

    private Iterator<ValueRange<T>> monsterIterator(final T rangeLow, final T rangeHigh, T maxDistance, final ValueRange<T> extremes, Authorizations auths) throws TableNotFoundException {

        //if the extremes range is larger than the max range then there is nothing left to do
        if (helper.distance(extremes).compareTo(maxDistance) >= 0)
            return emptyIterator();

        String outerBoundStart = helper.encode(helper.distance(new ValueRange<T>(maxDistance, rangeLow)));
        String outerBoundStop = helper.encodeComplement(rangeHigh);

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_REVERSE + DELIM + outerBoundStart + DELIM + outerBoundStop,
                INDEX_REVERSE + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and exhaust while trying to find a possible range that intersects.
        return new AbstractIterator<ValueRange<T>>() {
            @Override
            protected ValueRange<T> computeNext() {

                while (iterator.hasNext()) {
                    String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                    final T upper = helper.decodeComplement(vals[1]);
                    final T lower = helper.decode(vals[2]);

                    if(upper.compareTo(extremes.getStart()) < 0)
                        return endOfData();

                    if (lower.compareTo(rangeLow) < 0 && upper.compareTo(rangeLow) >= 0)
                        return new ValueRange<T>(lower, upper);

                }
                return endOfData();
            }
        };
    }

    private Iterator<ValueRange<T>> queryIterator(final T lowValue, final T highValue, final Authorizations auths) {

        try {
            //Iterate with a forward then a reverse iterator, while keeping track of the extremes.
            //After done iterating through then try to get a monster iterator to get the outliers
            // using the precomputed extremes.

            final ValueRange<T> extremes = new ValueRange<T>(lowValue, highValue);
            final Iterator<ValueRange<T>> mainIterator = concat(
                    forwardIterator(lowValue, highValue, auths, extremes),
                    reverseIterator(lowValue, highValue, auths, extremes)

            );
            final T maxDistance = getMaxDistance(auths);

            return new AbstractIterator<ValueRange<T>>() {

                Iterator<ValueRange<T>> monster = null;

                @Override
                protected ValueRange<T> computeNext() {
                    //exhaust the forward and reverse iterators
                    if (mainIterator.hasNext())
                        return mainIterator.next();
                    try {
                        //Now create the monster iterator after the extremes have been populated.
                        //Lazy init here to wait for the extremes to be populated correctly.
                        if (monster == null)
                            monster = monsterIterator(lowValue, highValue, maxDistance, extremes, auths);

                        if (monster.hasNext())
                            return monster.next();

                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    return endOfData();
                }
            };
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<ValueRange<T>> query(final ValueRange<T> range, final Auths auths) {
        checkNotNull(range);
        checkNotNull(auths);
        checkState(helper.isValid(range), "Invalid range.");

        return new Iterable<ValueRange<T>>() {
            @Override
            public Iterator<ValueRange<T>> iterator() {
                return queryIterator(range.getStart(), range.getStop(), auths.getAuths());
            }
        };
    }
}
