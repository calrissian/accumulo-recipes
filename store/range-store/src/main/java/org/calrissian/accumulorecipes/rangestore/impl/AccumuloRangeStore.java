package org.calrissian.accumulorecipes.rangestore.impl;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.rangestore.RangeStore;
import org.calrissian.mango.types.exception.TypeNormalizationException;
import org.calrissian.mango.types.normalizers.LongNormalizer;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.emptyIterator;
import static com.google.common.collect.Ranges.closed;
import static java.lang.Long.parseLong;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.data.Range.prefix;
import static org.calrissian.mango.collect.Iterables2.emptyIterable;

public class AccumuloRangeStore implements RangeStore {

    private static final String INDEX_FORWARD = "f";
    private static final String INDEX_REVERSE = "r";
    private static final String INDEX_MAXSIZE = "s";

    private static final LongNormalizer normalizer = new LongNormalizer();

    private static final String DELIM = "\u0000";

    private final Connector connector;
    private final String tableName;
    private final BatchWriter writer;

    public AccumuloRangeStore(Connector connector) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        this(connector, "ranges");
    }

    public AccumuloRangeStore(Connector connector, String tableName) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        checkNotNull(connector, "Invalid connector");
        checkNotNull(tableName, "The table name must not be empty");

        this.connector = connector;
        this.tableName = tableName;

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

    protected long complement(long value) {
        return Long.MAX_VALUE - value;
    }

    /**
     * Will close all underlying resources
     * @throws MutationsRejectedException
     */
    public void shutdown() throws MutationsRejectedException {
        writer.close();
    }

    @Override
    public void save(Iterable<Range<Long>> ranges) {
        checkNotNull(ranges);

        try {
            for (Range<Long> range : ranges) {
                //converts range to the form [a,b)
                range = range.canonical(DiscreteDomains.longs());
                if (range.isEmpty())
                    continue;

                final Long lowValue = range.lowerEndpoint();
                final Long highValue = (range.hasUpperBound() ? range.upperEndpoint() - 1 : Long.MAX_VALUE);

                String lowComplement = normalizer.normalize(complement(lowValue));
                String low = normalizer.normalize(lowValue);

                String highComplement = normalizer.normalize(complement(highValue));
                String high = normalizer.normalize(highValue);

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.put(new Text(""), new Text(""), new Value("".getBytes()));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.put(new Text(""), new Text(""), new Value("".getBytes()));

                String maxSizeComplement = normalizer.normalize(complement(highValue - lowValue));
                Mutation maxSize = new Mutation(INDEX_MAXSIZE + DELIM + maxSizeComplement);
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

    @Override
    public void delete(Iterable<Range<Long>> ranges) {
        checkNotNull(ranges);

        try {
            for (Range<Long> range : ranges) {
                //converts range to the form [a,b)
                range = range.canonical(DiscreteDomains.longs());
                if (range.isEmpty())
                    continue;

                final Long lowValue = range.lowerEndpoint();
                final Long highValue = (range.hasUpperBound() ? range.upperEndpoint() - 1 : Long.MAX_VALUE);

                String lowComplement = normalizer.normalize(complement(lowValue));
                String low = normalizer.normalize(lowValue);

                String highComplement = normalizer.normalize(complement(highValue));
                String high = normalizer.normalize(highValue);

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.putDelete(new Text(""), new Text(""));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.putDelete(new Text(""), new Text(""));

                String maxSizeComplement = normalizer.normalize(complement(highValue - lowValue));
                Mutation maxSize = new Mutation(INDEX_MAXSIZE + DELIM + maxSizeComplement);
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

    private long getMaxRangeSize(Authorizations auths) throws TableNotFoundException, TypeNormalizationException {
        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(prefix(INDEX_MAXSIZE));
        scanner.setBatchSize(1);

        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        if (!iterator.hasNext())
            return 0;

        //Only need the top one, as it should be sorted by size.
        Entry<Key,Value> entry = iterator.next();
        return complement(normalizer.denormalize(entry.getKey().getRow().toString().split(DELIM)[1]));
    }

    private Iterator<Range<Long>> forwardIterator(final Long rangeLow, final Long rangeHigh, Authorizations auths, final Mutable<Range<Long>> extremes) throws TableNotFoundException, TypeNormalizationException {

        String lowComplement = normalizer.normalize(complement(rangeLow));
        String high = normalizer.normalize(rangeHigh);

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_FORWARD + DELIM + high + DELIM + lowComplement, false,
                INDEX_FORWARD + "\uffff", true
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the low values exceed the high range mark.
        return new AbstractIterator<Range<Long>>() {
            @Override
            protected Range<Long> computeNext() {
                if (!iterator.hasNext())
                    return endOfData();

                String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                final Long upper = parseLong(vals[1]);
                final Long lower = complement(parseLong(vals[2]));

                //If we have gone past the high value then stop iterating
                if(lower > rangeHigh)
                    return endOfData();

                if (upper > extremes.get().upperEndpoint())
                    extremes.set(closed(extremes.get().lowerEndpoint(), upper));

                if (lower < extremes.get().lowerEndpoint())
                    extremes.set(closed(lower, extremes.get().upperEndpoint()));

                return closed(lower, upper);
            }
        };
    }

    private Iterator<Range<Long>> reverseIterator(final Long rangeLow, final Long rangeHigh, Authorizations auths, final Mutable<Range<Long>> extremes) throws TableNotFoundException, TypeNormalizationException {

        String low = normalizer.normalize(rangeLow);

        String highComplement = normalizer.normalize(complement(rangeHigh));

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_REVERSE + DELIM + highComplement + DELIM + low,
                INDEX_REVERSE + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the high values fall below the low range mark.
        return new AbstractIterator<Range<Long>>() {
            @Override
            protected Range<Long> computeNext() {
                if (!iterator.hasNext())
                    return endOfData();

                String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                final Long upper = complement(parseLong(vals[1]));
                final Long lower = parseLong(vals[2]);

                //If we have gone past the low value then stop iterating
                if(upper < rangeLow)
                    return endOfData();

                if (upper > extremes.get().upperEndpoint())
                    extremes.set(closed(extremes.get().lowerEndpoint(), upper));

                if (lower < extremes.get().lowerEndpoint())
                    extremes.set(closed(lower, extremes.get().upperEndpoint()));

                return closed(lower, upper);
            }
        };
    }

    private Iterator<Range<Long>> monsterIterator(final Long rangeLow, final Long rangeHigh, Long maxRangeSize, final Mutable<Range<Long>> extremes, Authorizations auths) throws TableNotFoundException, TypeNormalizationException {

        //if the extremes range is larger than the max range then there is nothing left to do
        if (extremes.get().upperEndpoint() - extremes.get().lowerEndpoint() >= maxRangeSize)
            return emptyIterator();

        String outerBoundStart = normalizer.normalize(rangeLow - maxRangeSize);
        String outerBoundStop = normalizer.normalize(complement(rangeHigh));

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_REVERSE + DELIM + outerBoundStart + DELIM + outerBoundStop,
                INDEX_REVERSE + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and exhaust while trying to find a possible range that intersects.
        return new AbstractIterator<Range<Long>>() {
            @Override
            protected Range<Long> computeNext() {

                while (iterator.hasNext()) {
                    String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                    final Long upper = complement(parseLong(vals[1]));
                    final Long lower = parseLong(vals[2]);

                    if(upper < extremes.get().upperEndpoint())
                        return endOfData();

                    if (lower < rangeLow && upper >= rangeLow)
                        return closed(lower, upper);

                }
                return endOfData();
            }
        };
    }

    private Iterator<Range<Long>> queryIterator(final Long lowValue, final Long highValue, final Authorizations auths) {

        try {
            //Iterate with a forward then a reverse iterator, while keeping track of the extremes.
            //After done iterating through then try to get a monster iterator to get the outliers
            // using the precomputed extremes.

            final Mutable<Range<Long>> extremes = new Mutable<Range<Long>>(closed(lowValue, highValue));
            final Iterator<Range<Long>> mainIterator = concat(
                    forwardIterator(lowValue, highValue, auths, extremes),
                    reverseIterator(lowValue, highValue, auths, extremes)

            );
            final long maxRangeSize = getMaxRangeSize(auths);

            return new AbstractIterator<Range<Long>>() {

                Iterator<Range<Long>> monster = null;

                @Override
                protected Range<Long> computeNext() {
                    //exhaust the forward and reverse iterators
                    if (mainIterator.hasNext())
                        return mainIterator.next();
                    try {
                        //Now create the monster iterator after the extremes have been populated.
                        //Lazy init here to wait for the extremes to be populated correctly.
                        if (monster == null)
                            monster = monsterIterator(lowValue, highValue, maxRangeSize, extremes, auths);

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

    @Override
    public Iterable<Range<Long>> query(Range<Long> range, final Authorizations auths) {

        //converts range to the form [a,b)
        range = range.canonical(DiscreteDomains.longs());
        if (range.isEmpty())
            return emptyIterable();

        final Long lowValue = range.lowerEndpoint();
        final Long highValue = (range.hasUpperBound() ? range.upperEndpoint() - 1 : Long.MAX_VALUE);

        return new Iterable<Range<Long>>() {
            @Override
            public Iterator<Range<Long>> iterator() {
                return queryIterator(lowValue, highValue, auths);
            }
        };
    }

    /**
     * Simply holds a reference to a piece of data to allow mutable data to be changed inside of anonymous iterators.
     * This is a hack.
     * @param <T>
     */
    private static class Mutable<T> {
        T value = null;

        public Mutable(T value) {
            this.value = value;
        }

        public T get() {
            return value;
        }

        public void set(T value) {
            this.value = value;
        }
    }
}
