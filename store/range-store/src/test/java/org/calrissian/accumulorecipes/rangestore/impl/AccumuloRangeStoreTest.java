package org.calrissian.accumulorecipes.rangestore.impl;


import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Ranges.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class AccumuloRangeStoreTest {

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    private static void compareRanges(Range<Long> expected, Range<Long> actual) {
        expected = expected.canonical(DiscreteDomains.longs());
        actual = actual.canonical(DiscreteDomains.longs());

        assertEquals(expected.lowerEndpoint(), actual.lowerEndpoint());
        assertEquals(expected.upperEndpoint(), actual.upperEndpoint());
    }

    @Test
    public void testSaveAndQuery() throws Exception {
        AccumuloRangeStore rangeStore = new AccumuloRangeStore(getConnector());
        Range<Long> testData = closed(0L, 10L);
        rangeStore.save(singleton(testData));

        List<Range<Long>> results = newArrayList(rangeStore.query(Ranges.<Long>all(), new Authorizations()));

        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));
    }

    @Test
    public void testBoundsWithQuery() throws Exception {
        AccumuloRangeStore rangeStore = new AccumuloRangeStore(getConnector());
        Range<Long> testData = closed(50L, 150L);
        rangeStore.save(singleton(testData));

        //testExact
        List<Range<Long>> results = newArrayList(rangeStore.query(closed(50L, 150L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //testSurrounding
        results = newArrayList(rangeStore.query(closed(49L, 151L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test within
        results = newArrayList(rangeStore.query(closed(90L, 110L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test high intersect
        results = newArrayList(rangeStore.query(closed(140L, 160L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test high touch
        results = newArrayList(rangeStore.query(closed(150L, 160L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test low intersect
        results = newArrayList(rangeStore.query(closed(40L, 60L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test low touch
        results = newArrayList(rangeStore.query(closed(40L, 60L), new Authorizations()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test outside low
        results = newArrayList(rangeStore.query(closed(0L, 49L), new Authorizations()));
        assertEquals(0, results.size());

        //test outside high
        results = newArrayList(rangeStore.query(closed(151L, 200L), new Authorizations()));
        assertEquals(0, results.size());
    }

    @Test
    public void testBoundsWithQueryAndMultRanges() throws Exception {
        AccumuloRangeStore rangeStore = new AccumuloRangeStore(getConnector());
        rangeStore.save(asList(closed(50L, 100L), closed(150L, 200L)));

        //test surrounding
        List<Range<Long>> results = newArrayList(rangeStore.query(Ranges.<Long>all(), new Authorizations()));
        assertEquals(2, results.size());

        //test both intersect
        results = newArrayList(rangeStore.query(closed(99L, 151L), new Authorizations()));
        assertEquals(2, results.size());

        //test both touch
        results = newArrayList(rangeStore.query(closed(100L, 150L), new Authorizations()));
        assertEquals(2, results.size());

        //test first intersect
        results = newArrayList(rangeStore.query(closed(99L, 101L), new Authorizations()));
        assertEquals(1, results.size());

        results = newArrayList(rangeStore.query(atMost(51L), new Authorizations()));
        assertEquals(1, results.size());

        //test second intersect
        results = newArrayList(rangeStore.query(closed(149L, 151L), new Authorizations()));
        assertEquals(1, results.size());

        results = newArrayList(rangeStore.query(atLeast(199L), new Authorizations()));
        assertEquals(1, results.size());

        //test within
        results = newArrayList(rangeStore.query(closed(101L, 149L), new Authorizations()));
        assertEquals(0, results.size());

        //test low
        results = newArrayList(rangeStore.query(atMost(0L), new Authorizations()));
        assertEquals(0, results.size());

        //test high
        results = newArrayList(rangeStore.query(atLeast(250L), new Authorizations()));
        assertEquals(0, results.size());


    }
}
