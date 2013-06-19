package org.calrissian.accumulorecipes.rangestore.impl;

import com.google.common.collect.AbstractIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.rangestore.RangeStore;
import org.calrissian.mango.types.exception.TypeNormalizationException;
import org.calrissian.mango.types.normalizers.LongNormalizer;
import org.calrissian.mango.types.range.ValueRange;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.emptyIterator;
import static java.lang.Long.parseLong;
import static java.util.Map.Entry;
import static org.apache.accumulo.core.data.Range.prefix;

public class AccumuloRangeStore implements RangeStore<Long> {

    private static final String INDEX_FORWARD = "f";
    private static final String INDEX_REVERSE = "r";
    private static final String INDEX_MAXDISTANCE = "s";

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
    public void save(Iterable<ValueRange<Long>> ranges) {
        checkNotNull(ranges);
        try {
            for (ValueRange<Long> range : ranges) {

                final Long lowValue = range.getStart();
                final Long highValue = range.getStop();

                checkState(lowValue >= 0, "No portion of a range can be less than 0");

                String lowComplement = normalizer.normalize(complement(lowValue));
                String low = normalizer.normalize(lowValue);

                String highComplement = normalizer.normalize(complement(highValue));
                String high = normalizer.normalize(highValue);

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.put(new Text(""), new Text(""), new Value("".getBytes()));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.put(new Text(""), new Text(""), new Value("".getBytes()));

                String distanceComplement = normalizer.normalize(complement(highValue - lowValue));
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

    @Override
    public void delete(Iterable<ValueRange<Long>> ranges) {
        checkNotNull(ranges);

        try {
            for (ValueRange<Long> range : ranges) {

                final Long lowValue = range.getStart();
                final Long highValue = range.getStop();

                checkState(lowValue >= 0, "No portion of a range can be less than 0");

                String lowComplement = normalizer.normalize(complement(lowValue));
                String low = normalizer.normalize(lowValue);

                String highComplement = normalizer.normalize(complement(highValue));
                String high = normalizer.normalize(highValue);

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.putDelete(new Text(""), new Text(""));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.putDelete(new Text(""), new Text(""));

                String distanceComplement = normalizer.normalize(complement(highValue - lowValue));
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

    private long getMaxDistance(Authorizations auths) throws TableNotFoundException, TypeNormalizationException {
        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(prefix(INDEX_MAXDISTANCE));
        scanner.setBatchSize(1);

        Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        if (!iterator.hasNext())
            return 0;

        //Only need the top one, as it should be sorted by size.
        Entry<Key,Value> entry = iterator.next();
        return complement(normalizer.denormalize(entry.getKey().getRow().toString().split(DELIM)[1]));
    }

    private Iterator<ValueRange<Long>> forwardIterator(final Long rangeLow, final Long rangeHigh, Authorizations auths, final ValueRange<Long> extremes) throws TableNotFoundException, TypeNormalizationException {

        String lowComplement = normalizer.normalize(complement(rangeLow));
        String high = normalizer.normalize(rangeHigh);

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_FORWARD + DELIM + high + DELIM + lowComplement, false,
                INDEX_FORWARD + "\uffff", true
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the low values exceed the high range mark.
        return new AbstractIterator<ValueRange<Long>>() {
            @Override
            protected ValueRange<Long> computeNext() {
                if (!iterator.hasNext())
                    return endOfData();

                String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                final Long upper = parseLong(vals[1]);
                final Long lower = complement(parseLong(vals[2]));

                //If we have gone past the high value then stop iterating
                if(lower > rangeHigh)
                    return endOfData();

                if (upper > extremes.getStop())
                    extremes.setStop(upper);

                if (lower < extremes.getStart())
                    extremes.setStart(lower);

                return new ValueRange<Long> (lower, upper);
            }
        };
    }

    private Iterator<ValueRange<Long>> reverseIterator(final Long rangeLow, final Long rangeHigh, Authorizations auths, final ValueRange<Long> extremes) throws TableNotFoundException, TypeNormalizationException {

        String low = normalizer.normalize(rangeLow);

        String highComplement = normalizer.normalize(complement(rangeHigh));

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_REVERSE + DELIM + highComplement + DELIM + low,
                INDEX_REVERSE + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and stop iterating after the high values fall below the low range mark.
        return new AbstractIterator<ValueRange<Long>>() {
            @Override
            protected ValueRange<Long> computeNext() {
                if (!iterator.hasNext())
                    return endOfData();

                String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                final Long upper = complement(parseLong(vals[1]));
                final Long lower = parseLong(vals[2]);

                //If we have gone past the low value then stop iterating
                if(upper < rangeLow)
                    return endOfData();

                if (upper > extremes.getStop())
                    extremes.setStop(upper);

                if (lower < extremes.getStart())
                    extremes.setStart(lower);

                return new ValueRange<Long>(lower, upper);
            }
        };
    }

    private Iterator<ValueRange<Long>> monsterIterator(final Long rangeLow, final Long rangeHigh, Long maxDistance, final ValueRange<Long> extremes, Authorizations auths) throws TableNotFoundException, TypeNormalizationException {

        //if the extremes range is larger than the max range then there is nothing left to do
        if (extremes.getStop() - extremes.getStart() >= maxDistance)
            return emptyIterator();

        String outerBoundStart = normalizer.normalize(rangeLow - maxDistance);
        String outerBoundStop = normalizer.normalize(complement(rangeHigh));

        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new org.apache.accumulo.core.data.Range(
                INDEX_REVERSE + DELIM + outerBoundStart + DELIM + outerBoundStop,
                INDEX_REVERSE + "\uffff"
        ));
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        //Transform data into ranges and exhaust while trying to find a possible range that intersects.
        return new AbstractIterator<ValueRange<Long>>() {
            @Override
            protected ValueRange<Long> computeNext() {

                while (iterator.hasNext()) {
                    String vals[] = iterator.next().getKey().getRow().toString().split(DELIM);
                    final Long upper = complement(parseLong(vals[1]));
                    final Long lower = parseLong(vals[2]);

                    if(upper < extremes.getStart())
                        return endOfData();

                    if (lower < rangeLow && upper >= rangeLow)
                        return new ValueRange<Long>(lower, upper);

                }
                return endOfData();
            }
        };
    }

    private Iterator<ValueRange<Long>> queryIterator(final Long lowValue, final Long highValue, final Authorizations auths) {

        try {
            //Iterate with a forward then a reverse iterator, while keeping track of the extremes.
            //After done iterating through then try to get a monster iterator to get the outliers
            // using the precomputed extremes.

            final ValueRange<Long> extremes = new ValueRange<Long>(lowValue, highValue);
            final Iterator<ValueRange<Long>> mainIterator = concat(
                    forwardIterator(lowValue, highValue, auths, extremes),
                    reverseIterator(lowValue, highValue, auths, extremes)

            );
            final long maxDistance = getMaxDistance(auths);

            return new AbstractIterator<ValueRange<Long>>() {

                Iterator<ValueRange<Long>> monster = null;

                @Override
                protected ValueRange<Long> computeNext() {
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

    @Override
    public Iterable<ValueRange<Long>> query(ValueRange<Long> range, final Authorizations auths) {

        final Long lowValue = range.getStart();
        final Long highValue = range.getStop();

        checkState(lowValue >= 0, "No portion of a range can be less than 0");

        return new Iterable<ValueRange<Long>>() {
            @Override
            public Iterator<ValueRange<Long>> iterator() {
                return queryIterator(lowValue, highValue, auths);
            }
        };
    }
}
