package org.calrissian.accumulorecipes.rangestore.impl;


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.mango.types.range.ValueRange;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class AccumuloRangeStoreTest {

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    private static void compareRanges(ValueRange<Long> expected, ValueRange<Long> actual) {

        assertEquals(expected.getStart(), actual.getStart());
        assertEquals(expected.getStop(), actual.getStop());
    }

    @Test
    public void testSaveAndQuery() throws Exception {
        AccumuloRangeStore rangeStore = new AccumuloRangeStore(getConnector());
        ValueRange<Long> testData = new ValueRange<Long>(0L, 10L);
        rangeStore.save(singleton(testData));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, Long.MAX_VALUE), new Authorizations()));

        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));
    }

    @Test
    public void testBoundsWithQuery() throws Exception {
        AccumuloRangeStore rangeStore = new AccumuloRangeStore(getConnector());
        ValueRange<Long> testData = new ValueRange<Long>(50L, 150L);
        rangeStore.save(singleton(testData));

        //testExact
        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(50L, 150L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //testSurrounding
        results = newArrayList(rangeStore.query(new ValueRange<Long>(49L, 151L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test within
        results = newArrayList(rangeStore.query(new ValueRange<Long>(90L, 110L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test high intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(140L, 160L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test high touch
        results = newArrayList(rangeStore.query(new ValueRange<Long>(150L, 160L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test low intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(40L, 60L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test low touch
        results = newArrayList(rangeStore.query(new ValueRange<Long>(40L, 60L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test outside low
        results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 49L), new Authorizations()));
        assertEquals(0, results.size());

        //test outside high
        results = newArrayList(rangeStore.query(new ValueRange<Long>(151L, 200L), new Authorizations()));
        assertEquals(0, results.size());
    }

    @Test
    public void testBoundsWithQueryAndMultRanges() throws Exception {
        AccumuloRangeStore rangeStore = new AccumuloRangeStore(getConnector());
        rangeStore.save(asList(new ValueRange<Long>(50L, 100L), new ValueRange<Long>(150L, 200L)));

        //test surrounding
        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, Long.MAX_VALUE), new Authorizations()));
        assertEquals(2, results.size());

        //test both intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(99L, 151L), new Authorizations()));
        assertEquals(2, results.size());

        //test both touch
        results = newArrayList(rangeStore.query(new ValueRange<Long>(100L, 150L), new Authorizations()));
        assertEquals(2, results.size());

        //test first intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(99L, 101L), new Authorizations()));
        assertEquals(1, results.size());

        results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 51L), new Authorizations()));
        assertEquals(1, results.size());

        //test second intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(149L, 151L), new Authorizations()));
        assertEquals(1, results.size());

        results = newArrayList(rangeStore.query(new ValueRange<Long>(199L, Long.MAX_VALUE), new Authorizations()));
        assertEquals(1, results.size());

        //test within
        results = newArrayList(rangeStore.query(new ValueRange<Long>(101L, 149L), new Authorizations()));
        assertEquals(0, results.size());

        //test low
        results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 10L), new Authorizations()));
        assertEquals(0, results.size());

        //test high
        results = newArrayList(rangeStore.query(new ValueRange<Long>(250L, Long.MAX_VALUE), new Authorizations()));
        assertEquals(0, results.size());


    }
}
