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
package org.calrissian.accumulorecipes.metricsstore.impl;


import com.google.common.collect.AbstractIterator;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class AccumuloMetricStoreTest {

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    public static long getTimeOffset(MetricTimeUnit timeUnit){
        switch (timeUnit) {
            case MINUTES: return TimeUnit.MINUTES.toMillis(1);
            case HOURS: return TimeUnit.HOURS.toMillis(1);
            case DAYS: return TimeUnit.DAYS.toMillis(1);
            case MONTHS: return TimeUnit.DAYS.toMillis(31);
        }
        return TimeUnit.MINUTES.toMillis(1);
    }

    public static Iterable<Metric> generateTestData(MetricTimeUnit timeUnit, int limit) {
        final long startTime = System.currentTimeMillis();
        final long offset = getTimeOffset(timeUnit);

        return limit(
            new Iterable<Metric>() {
                @Override
                public Iterator<Metric> iterator() {
                    return new AbstractIterator<Metric>() {
                        long current = startTime;

                        @Override
                        protected Metric computeNext() {
                            current -= offset;
                            return new Metric(current, "group", "type", "name", "", 1);
                        }
                    };
                }
            },
            limit);
    }

    public static void checkMetrics(Iterable<Metric> actual, int expectedNum, int expectedVal) {
        List<Metric> actualList = newArrayList(actual);

        assertEquals(expectedNum, actualList.size());

        for (Metric metric : actualList) {
            assertEquals("group", metric.getGroup());
            assertEquals("type", metric.getType());
            assertEquals("name", metric.getName());
            assertEquals("", metric.getVisibility());
            assertEquals(expectedVal, metric.getValue());
        }
    }

    @Test
    public void testStoreAndQuery() throws Exception {
        AccumuloMetricStore metricStore = new AccumuloMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);

        Iterable<Metric> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());

        checkMetrics(actual, 60, 1);
    }

    @Test
    public void testQueryAggregation() throws Exception {

        Connector connector = getConnector();
        AccumuloMetricStore metricStore = new AccumuloMetricStore(connector);

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<Metric> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());

        checkMetrics(actual, 60, 3);
    }
}
