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
package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl;


import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.SummaryStatsFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.StatsFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.SumFunction;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStoreTest.generateTestData;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStoreTest.getConnector;
import static org.calrissian.mango.collect.CloseableIterables.autoClose;
import static org.junit.Assert.assertEquals;

public class AccumuloCustomMetricStoreTest {



    private static void checkCustom(CloseableIterable<CustomMetric<Long>> actual, int expectedNum, Long expectedVal) {
        List<CustomMetric<Long>> actualList = newArrayList(autoClose(actual));

        assertEquals(expectedNum, actualList.size());

        for (CustomMetric<Long> metric : actualList) {
            assertEquals("group", metric.getGroup());
            assertEquals("type", metric.getType());
            assertEquals("name", metric.getName());
            assertEquals("", metric.getVisibility());
            assertEquals(expectedVal, metric.getValue());
        }
    }

    private static void checkCustomStats(Iterable<CustomMetric<long[]>> actual, int expectedNum, long expectedVal) {
        List<CustomMetric<long[]>> actualList = newArrayList(actual);

        assertEquals(expectedNum, actualList.size());

        for (CustomMetric<long[]> metric : actualList) {
            assertEquals("group", metric.getGroup());
            assertEquals("type", metric.getType());
            assertEquals("name", metric.getName());
            assertEquals("", metric.getVisibility());
            assertEquals(1, metric.getValue()[0]);
            assertEquals(1, metric.getValue()[1]);
            assertEquals(expectedVal, metric.getValue()[2]);
            assertEquals(expectedVal, metric.getValue()[3]);
        }
    }

    private static void checkSummaryStats(Iterable<CustomMetric<SummaryStatistics>> actual, int expectedNum, long expectedVal) {
        List<CustomMetric<SummaryStatistics>> actualList = newArrayList(actual);

        assertEquals(expectedNum, actualList.size());

        for (CustomMetric<SummaryStatistics> metric : actualList) {
            assertEquals("group", metric.getGroup());
            assertEquals("type", metric.getType());
            assertEquals("name", metric.getName());
            assertEquals("", metric.getVisibility());
            assertEquals(1, metric.getValue().getMin(), 0.00000001);
            assertEquals(1, metric.getValue().getMax(), 0.00000001);
            assertEquals(1.0, metric.getValue().getMean(), 0.00000001);
            assertEquals(0.0, metric.getValue().getStandardDeviation(), 0.00000001);
            assertEquals(0.0, metric.getValue().getVariance(), 0.00000001);
            assertEquals(expectedVal, metric.getValue().getN());
        }
    }

    @Test
    public void testStoreAndQuery() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);

        CloseableIterable<CustomMetric<Long>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", SumFunction.class,  MetricTimeUnit.MINUTES, new Auths());

        checkCustom(actual, 60, 1L);
    }

    @Test
    public void testQueryAggregation() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        CloseableIterable<CustomMetric<Long>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", SumFunction.class, MetricTimeUnit.MINUTES, new Auths());

        checkCustom(actual, 60, 3L);
    }

    @Test
    public void testQueryAggregationComplex() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<CustomMetric<long[]>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", StatsFunction.class, MetricTimeUnit.MINUTES, new Auths());

        checkCustomStats(actual, 60, 3);
    }

    @Test
    public void testQueryComplexFunction() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<CustomMetric<SummaryStatistics>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", SummaryStatsFunction.class, MetricTimeUnit.MINUTES, new Auths());

        checkSummaryStats(actual, 60, 3);
    }
}
