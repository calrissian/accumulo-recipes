package org.calrissian.recipes.accumulo.rangestore.impl;

import mango.types.exception.TypeNormalizationException;
import mango.types.normalizers.LongNormalizer;
import mango.types.range.ValueRange;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.recipes.accumulo.rangestore.RangeStore;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class AccumuloRangeStore implements RangeStore {

    public static final String INDEX_FORWARD = "f";
    public static final String INDEX_REVERSE = "r";
    public static final String INDEX_MAXSIZE = "s";

    public static final String DELIM = "\u0000";

    protected String tableName = "ranges";

    protected Connector connector;
    protected BatchWriter writer;
    protected final LongNormalizer normalizer = new LongNormalizer();


    public AccumuloRangeStore(Connector connector) {

        this.connector = connector;
    }

    public void initialize() {

        if(!connector.tableOperations().exists(tableName)) {
            try {
                connector.tableOperations().create(tableName);
            } catch (Exception e) {

                e.printStackTrace();
            }
        }

        try {
            writer = connector.createBatchWriter(tableName, 10000L, 10000L, 10);
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void insert(Collection<ValueRange<Long>> ranges) {

        try {

            for(ValueRange<Long> range : ranges) {

                String lowComplement =  normalizer.normalize(Long.MAX_VALUE - range.getStart());
                String low = normalizer.normalize(range.getStart());

                String highComplement = normalizer.normalize(Long.MAX_VALUE - range.getStop());
                String high = normalizer.normalize(range.getStop());

                Mutation forward = new Mutation(INDEX_FORWARD + DELIM + high + DELIM + lowComplement);
                forward.put(new Text(""), new Text(""), new Value("".getBytes()));

                Mutation reverse = new Mutation(INDEX_REVERSE + DELIM + highComplement + DELIM + low);
                reverse.put(new Text(""), new Text(""), new Value("".getBytes()));

                String maxSizeComplement = normalizer.normalize(Long.MAX_VALUE - (range.getStop() - range.getStart()));
                Mutation maxSize = new Mutation(INDEX_MAXSIZE + DELIM + maxSizeComplement);
                maxSize.put(new Text(low), new Text(high), new Value("".getBytes()));

                writer.addMutation(forward);
                writer.addMutation(reverse);
                writer.addMutation(maxSize);
            }

            writer.flush();

        } catch (TypeNormalizationException e) {
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<ValueRange<Long>> query(final ValueRange<Long> range, Authorizations auths) {

        try {

            String lowComplement =  normalizer.normalize(Long.MAX_VALUE - range.getStart());
            String low = normalizer.normalize(range.getStart());

            String highComplement = normalizer.normalize(Long.MAX_VALUE - range.getStop());
            String high = normalizer.normalize(range.getStop());

            final Scanner forwardScanner = connector.createScanner(tableName, auths);
            final Scanner reverseScanner = connector.createScanner(tableName, auths);
            Scanner maxSizeScanner = connector.createScanner(tableName, auths);

            Range forwardRange = new Range(INDEX_FORWARD + DELIM + high + DELIM + lowComplement, false, INDEX_FORWARD + "\uffff", true);
            Range reverseRange = new Range(INDEX_REVERSE + DELIM + highComplement + DELIM + low, INDEX_REVERSE + "\uffff");
            Range maxSizeRange = new Range(INDEX_MAXSIZE, INDEX_MAXSIZE + "\uffff");

            forwardScanner.setRange(forwardRange);
            reverseScanner.setRange(reverseRange);
            maxSizeScanner.setRange(maxSizeRange);

            final Iterator<Map.Entry<Key,Value>> forwardIterator = forwardScanner.iterator();
            final Iterator<Map.Entry<Key,Value>> reverseIterator = reverseScanner.iterator();

            Long intervalMaxSize = 0L;
            Iterator<Map.Entry<Key,Value>> maxSizeItr = maxSizeScanner.iterator();
            if(maxSizeItr.hasNext()) {

                Map.Entry<Key,Value> entry = maxSizeItr.next();
                intervalMaxSize = Long.MAX_VALUE - normalizer.denormalize(entry.getKey().getRow().toString().split(DELIM)[1]);
            }

            final Long maxSizeLong = intervalMaxSize;

            return new Iterator<ValueRange<Long>>() {

                ValueRange<Long> nextValue;

                Long fullLength = range.getStop() - range.getStart();
                //
                Long outerBoundStart = range.getStart() - maxSizeLong;
                Long outerBoundStop = Long.MAX_VALUE - range.getStop();

                boolean forwardIteratorDone = false;
                boolean reverseIteratorDone = false;
                boolean monsterRangeDone = false;

                ValueRange<Long> lowest;
                ValueRange<Long> highest;

                Iterator<Map.Entry<Key,Value>> monsterIterator = null;


                @Override
                public boolean hasNext() {

                    if(nextValue != null) {
                        return true;
                    }

                    else if(forwardIteratorDone && reverseIteratorDone && monsterRangeDone) {
                        return false;
                    }

                    primeNext();

                    return nextValue != null;
                }

                @Override
                public ValueRange<Long> next() {

                    if(hasNext()) {

                        ValueRange<Long> retVal = nextValue;
                        nextValue = null;

                        return retVal;
                    }

                    else {
                        throw new RuntimeException("Iterator has been exhausted");
                    }

                }

                @Override
                public void remove() {

                    forwardIterator.remove();
                    reverseIterator.remove();
                }

                private void primeNext() {



                    if((!forwardIteratorDone && forwardIterator.hasNext())){

                        Map.Entry<Key,Value> next = forwardIterator.next();

                        String vals[] = next.getKey().getRow().toString().split(DELIM);

                        final Long high = Long.parseLong(vals[1]);
                        final Long low = Long.MAX_VALUE - Long.parseLong(vals[2]);

                        if(low > range.getStop()) {

                            forwardIteratorDone = true;
                        }

                        else {
                            nextValue = new ValueRange<Long>(low, high);
                        }
                    }

                    else {
                        forwardIteratorDone = true;
                    }

                    if(forwardIteratorDone && reverseIterator.hasNext()) {

                        Map.Entry<Key,Value> next = reverseIterator.next();
                        String vals[] = next.getKey().getRow().toString().split(DELIM);

                        final Long high = Long.MAX_VALUE - Long.parseLong(vals[1]);
                        final Long low = Long.parseLong(vals[2]);

                        if(high < range.getStart()) {

                            reverseIteratorDone = true;
                        }

                        else {
                            nextValue = new ValueRange<Long>(low, high);
                        }
                    }

                    else if(forwardIteratorDone) {
                        reverseIteratorDone = true;
                    }

                    if(reverseIteratorDone && monsterIterator == null) {

                        if(fullLength < maxSizeLong) {
                            Range reverseRange = new Range(INDEX_REVERSE + DELIM + outerBoundStart + DELIM + outerBoundStop, INDEX_REVERSE + "\uffff");
                            reverseScanner.clearScanIterators();
                            reverseScanner.setRange(reverseRange);

                            monsterIterator = reverseScanner.iterator();

                        }

                        else {
                            monsterRangeDone = true;
                        }

                    }

                    if(reverseIteratorDone && monsterIterator != null && monsterIterator.hasNext()) {

                        while(monsterIterator.hasNext()) {

                            Map.Entry<Key,Value> next = monsterIterator.next();
                            String vals[] = next.getKey().getRow().toString().split(DELIM);

                            final Long high = Long.MAX_VALUE - Long.parseLong(vals[1]);
                            final Long low = Long.parseLong(vals[2]);

                            if(high <= (highest != null ? highest.getStop() : range.getStop())) {

                                monsterRangeDone = true;
                                break;
                            }

                            else if(low < range.getStart() && high >= range.getStart()) {

                                nextValue = new ValueRange<Long>(low, high);
                                break;
                            }
                        }
                    }

                    else {
                        monsterRangeDone = true;
                    }

                    if(!(forwardIteratorDone && reverseIteratorDone)) {
                        if(lowest == null) {
                            lowest = nextValue;
                        }

                        if(highest == null) {
                            highest = nextValue;
                        }

                        if(nextValue != null) {

                            if(nextValue.getStart() < lowest.getStart()) {
                                lowest = nextValue;

                            }

                            if(nextValue.getStart() > highest.getStart()) {
                                highest = nextValue;
                            }

                            fullLength = highest.getStop() - lowest.getStart();
                        }
                    }
                }
            };

        } catch (TableNotFoundException e) {
            e.printStackTrace();
        } catch (TypeNormalizationException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void shutdown() {
        try {
            writer.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

}
