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


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.rangestore.helper.LongRangeHelper;
import org.calrissian.mango.domain.ValueRange;
import org.junit.Ignore;
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

    @Ignore
    @Test
    public void testStumbleForwardIterator() throws Exception{
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(80L, 90L)));
        rangeStore.save(singleton(new ValueRange<Long>(50L, 100L)));
        rangeStore.save(singleton(new ValueRange<Long>(50L, 75L)));

        //should return [2-98] and [20-80]
        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(49L, 51L), new Auths()));

        //actually returns [50-100], [50-75] because the forward and monster iterator both pick up 20-80
        assertEquals(2, results.size());
    }

    @Ignore
    @Test
    public void testGoofyMonsterRange() throws Exception{
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(5L, 10L)));
        rangeStore.save(singleton(new ValueRange<Long>(90L, 95L)));
        rangeStore.save(singleton(new ValueRange<Long>(2L, 98L)));
        rangeStore.save(singleton(new ValueRange<Long>(20L, 80L)));

        //should return [2-98] and [20-80]
        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(49L, 51L), new Auths()));

        //actually returns [20-80], [2-98], [20-80] because the forward and monster iterator both pick up 20-80
        assertEquals(2, results.size());
        compareRanges(new ValueRange<Long>(1L, 4L), results.get(0));
    }

    @Test
    public void testSaveAndQuery() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());
        ValueRange<Long> testData = new ValueRange<Long>(0L, 10L);
        rangeStore.save(singleton(testData));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, Long.MAX_VALUE), new Auths()));

        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));
    }

    @Test
    public void testQuerySingleRangeExpected() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(1L, 4L)));
        rangeStore.save(singleton(new ValueRange<Long>(3L, 7L)));
        rangeStore.save(singleton(new ValueRange<Long>(8L, 9L)));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(1L, 2L), new Auths()));

        assertEquals(1, results.size());
        compareRanges(new ValueRange<Long>(1L, 4L), results.get(0));
    }

    @Test
    public void testQueryTwoRangeOverlapExpected() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(1L, 4L)));
        rangeStore.save(singleton(new ValueRange<Long>(3L, 7L)));
        rangeStore.save(singleton(new ValueRange<Long>(8L, 9L)));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(1L, 4L), new Auths()));

        assertEquals(2, results.size());
        compareRanges(new ValueRange<Long>(3L, 7L), results.get(0));
        compareRanges(new ValueRange<Long>(1L, 4L), results.get(1));
    }

    @Test
    public void testQueryEntireRangeExpected() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(50L, 57L)));
        rangeStore.save(singleton(new ValueRange<Long>(62L, 70L)));
        rangeStore.save(singleton(new ValueRange<Long>(0L, 500L)));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 500L), new Auths()));

        assertEquals(3, results.size());
        compareRanges(new ValueRange<Long>(0L, 500L), results.get(0));
        compareRanges(new ValueRange<Long>(62L, 70L), results.get(1));
        compareRanges(new ValueRange<Long>(50L, 57L), results.get(2));
    }

    @Test
    public void testQuerySingleMonsterOutsideRangeExpected() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(50L, 57L)));
        rangeStore.save(singleton(new ValueRange<Long>(62L, 70L)));
        rangeStore.save(singleton(new ValueRange<Long>(80L, 80L)));
        rangeStore.save(singleton(new ValueRange<Long>(81L, 89L)));
        rangeStore.save(singleton(new ValueRange<Long>(-4000L, 4000L)));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(75L, 75L), new Auths()));

        assertEquals(1, results.size());
        compareRanges(new ValueRange<Long>(-4000L, 4000L), results.get(0));
    }

    @Test
    public void testQueryMultipleMonsterOutsideRangeExpected() throws Exception {

        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(50L, 57L)));
        rangeStore.save(singleton(new ValueRange<Long>(62L, 70L)));
        rangeStore.save(singleton(new ValueRange<Long>(80L, 80L)));
        rangeStore.save(singleton(new ValueRange<Long>(81L, 89L)));
        rangeStore.save(singleton(new ValueRange<Long>(-4000L, 4000L)));
        rangeStore.save(singleton(new ValueRange<Long>(-100000L, 500000L)));

        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(75L, 75L), new Auths()));

        assertEquals(2, results.size());
        compareRanges(new ValueRange<Long>(-100000L, 500000L), results.get(0));
        compareRanges(new ValueRange<Long>(-4000L, 4000L), results.get(1));
    }

    @Test
    public void testQueryExactRangeExpected() throws Exception {

        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(1L, 4L)));
        rangeStore.save(singleton(new ValueRange<Long>(3L, 7L)));
        rangeStore.save(singleton(new ValueRange<Long>(8L, 9L)));


        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(8L, 9L), new Auths()));

        assertEquals(1, results.size());
        compareRanges(new ValueRange<Long>(8L, 9L), results.get(0));
    }

    @Test
    public void testQueryInsideRangeExpected() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(1L, 4L)));
        rangeStore.save(singleton(new ValueRange<Long>(3L, 7L)));
        rangeStore.save(singleton(new ValueRange<Long>(8L, 9L)));


        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(5L, 6L), new Auths()));

        assertEquals(1, results.size());
        compareRanges(new ValueRange<Long>(3L, 7L), results.get(0));
    }

    @Test
    public void testQuerySinglePointExpected() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(1L, 4L)));
        rangeStore.save(singleton(new ValueRange<Long>(3L, 7L)));
        rangeStore.save(singleton(new ValueRange<Long>(8L, 9L)));


        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(1L, 1L), new Auths()));

        assertEquals(1, results.size());
        compareRanges(new ValueRange<Long>(1L, 4L), results.get(0));
    }

    @Test
    public void testQuerySinglePointNothingExpected() throws Exception {

        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());

        rangeStore.save(singleton(new ValueRange<Long>(2L, 3L)));
        rangeStore.save(singleton(new ValueRange<Long>(6L, 15L)));
        rangeStore.save(singleton(new ValueRange<Long>(20L, 27L)));


        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(4L, 4L), new Auths()));

        assertEquals(0, results.size());
    }

    @Test
    public void testBoundsWithQuery() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());
        ValueRange<Long> testData = new ValueRange<Long>(50L, 150L);
        rangeStore.save(singleton(testData));

        //testExact
        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(50L, 150L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //testSurrounding
        results = newArrayList(rangeStore.query(new ValueRange<Long>(49L, 151L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test within
        results = newArrayList(rangeStore.query(new ValueRange<Long>(90L, 110L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test high intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(140L, 160L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test high touch
        results = newArrayList(rangeStore.query(new ValueRange<Long>(150L, 160L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test low intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(40L, 60L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test low touch
        results = newArrayList(rangeStore.query(new ValueRange<Long>(40L, 60L), new Auths()));
        assertEquals(1, results.size());
        compareRanges(testData, results.get(0));

        //test outside low
        results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 49L), new Auths()));
        assertEquals(0, results.size());

        //test outside high
        results = newArrayList(rangeStore.query(new ValueRange<Long>(151L, 200L), new Auths()));
        assertEquals(0, results.size());
    }

    @Test
    public void testBoundsWithQueryAndMultRanges() throws Exception {
        AccumuloRangeStore<Long> rangeStore = new AccumuloRangeStore<Long>(getConnector(), new LongRangeHelper());
        rangeStore.save(asList(new ValueRange<Long>(50L, 100L), new ValueRange<Long>(150L, 200L)));

        //test surrounding
        List<ValueRange<Long>> results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, Long.MAX_VALUE), new Auths()));
        assertEquals(2, results.size());

        //test both intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(99L, 151L), new Auths()));
        assertEquals(2, results.size());

        //test both touch
        results = newArrayList(rangeStore.query(new ValueRange<Long>(100L, 150L), new Auths()));
        assertEquals(2, results.size());

        //test first intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(99L, 101L), new Auths()));
        assertEquals(1, results.size());

        results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 51L), new Auths()));
        assertEquals(1, results.size());

        //test second intersect
        results = newArrayList(rangeStore.query(new ValueRange<Long>(149L, 151L), new Auths()));
        assertEquals(1, results.size());

        results = newArrayList(rangeStore.query(new ValueRange<Long>(199L, Long.MAX_VALUE), new Auths()));
        assertEquals(1, results.size());

        //test within
        results = newArrayList(rangeStore.query(new ValueRange<Long>(101L, 149L), new Auths()));
        assertEquals(0, results.size());

        //test low
        results = newArrayList(rangeStore.query(new ValueRange<Long>(0L, 10L), new Auths()));
        assertEquals(0, results.size());

        //test high
        results = newArrayList(rangeStore.query(new ValueRange<Long>(250L, Long.MAX_VALUE), new Auths()));
        assertEquals(0, results.size());


    }
}
