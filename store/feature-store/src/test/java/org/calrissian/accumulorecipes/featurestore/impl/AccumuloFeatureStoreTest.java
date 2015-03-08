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
package org.calrissian.accumulorecipes.featurestore.impl;


import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.Metric;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singleton;
import static org.calrissian.mango.collect.CloseableIterables.autoClose;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloFeatureStoreTest {

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    public static long getTimeOffset(TimeUnit timeUnit) {
        switch (timeUnit) {
            case MINUTES:
                return java.util.concurrent.TimeUnit.MINUTES.toMillis(1);
            case HOURS:
                return java.util.concurrent.TimeUnit.HOURS.toMillis(1);
            case DAYS:
                return java.util.concurrent.TimeUnit.DAYS.toMillis(1);
            case MONTHS:
                return java.util.concurrent.TimeUnit.DAYS.toMillis(31);
        }
        return java.util.concurrent.TimeUnit.MINUTES.toMillis(1);
    }

    public static Iterable<MetricFeature> generateTestData(TimeUnit timeUnit, int limit) {
        final long startTime = System.currentTimeMillis();
        final long offset = getTimeOffset(timeUnit);

        return limit(
                new Iterable<MetricFeature>() {
                    @Override
                    public Iterator<MetricFeature> iterator() {
                        return new AbstractIterator<MetricFeature>() {
                            long current = startTime;

                            @Override
                            protected MetricFeature computeNext() {
                                current -= offset;
                                return new MetricFeature(current, "group", "type", "name", "", new Metric(1));
                            }
                        };
                    }
                },
                limit
        );
    }

    public static void checkMetrics(CloseableIterable<MetricFeature> actual, int expectedNum, int expectedVal) {
        List<MetricFeature> actualList = newArrayList(autoClose(actual));

        assertEquals(expectedNum, actualList.size());

        for (MetricFeature metric : actualList) {
            assertEquals("group", metric.getGroup());
            assertEquals("type", metric.getType());
            assertEquals("name", metric.getName());
            assertEquals("", metric.getVisibility());
            assertEquals(expectedVal, metric.getVector().getSum());
        }
    }

    @Test
    public void testStoreAndQuery() throws Exception {
        AccumuloFeatureStore metricStore = new AccumuloFeatureStore(getConnector());
        metricStore.initialize();

        Iterable<MetricFeature> testData = generateTestData(TimeUnit.MINUTES, 60);

        metricStore.save(testData);

        CloseableIterable<MetricFeature> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", TimeUnit.MINUTES, MetricFeature.class, Auths.EMPTY);

        checkMetrics(actual, 60, 1);
    }


    @Test
    public void testStoreAndQuery_specificTimeUnits() throws Exception {
        AccumuloFeatureStore metricStore = new AccumuloFeatureStore(getConnector());
        metricStore.initialize();

        Iterable<MetricFeature> testData = generateTestData(TimeUnit.MINUTES, 60);
        metricStore.save(testData, singleton(TimeUnit.MINUTES));

        CloseableIterable<MetricFeature> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", TimeUnit.MINUTES, MetricFeature.class, Auths.EMPTY);
        checkMetrics(actual, 60, 1);

        actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", TimeUnit.HOURS, MetricFeature.class, Auths.EMPTY);
        assertEquals(0, Iterables.size(actual));

        actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", TimeUnit.DAYS, MetricFeature.class, Auths.EMPTY);
        assertEquals(0, Iterables.size(actual));

        actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", TimeUnit.MONTHS, MetricFeature.class, Auths.EMPTY);
        assertEquals(0, Iterables.size(actual));

    }

    @Test
    public void testQueryAggregation() throws Exception {

        Connector connector = getConnector();
        AccumuloFeatureStore metricStore = new AccumuloFeatureStore(connector);
        metricStore.initialize();
        Iterable<MetricFeature> testData = generateTestData(TimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);
        AccumuloTestUtils.dumpTable(connector, "features_reverse");


        CloseableIterable<MetricFeature> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", TimeUnit.MINUTES, MetricFeature.class, Auths.EMPTY);

        checkMetrics(actual, 60, 3);
    }

    @Test
    public void testExceptionThrownWhenNotInitialized() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Connector connector = getConnector();
        AccumuloFeatureStore metricStore = new AccumuloFeatureStore(connector);
        Iterable<MetricFeature> testData = generateTestData(TimeUnit.MINUTES, 60);

        try {
            metricStore.save(testData);
            fail("Should have failed while store wasn't initialized()");
        } catch(Exception e) {}

        try {
            metricStore.query(new Date(), new Date(), "group", "type", "name", TimeUnit.DAYS, MetricFeature.class, Auths.EMPTY);
            fail("Should have failed while store wasn't initialized()");
        } catch(Exception e) {}
    }
}
