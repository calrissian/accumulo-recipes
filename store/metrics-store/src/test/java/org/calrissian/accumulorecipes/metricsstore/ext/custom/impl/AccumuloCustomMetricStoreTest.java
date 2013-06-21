/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
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


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.StatsFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.SummingFunction;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStoreTest.generateTestData;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStoreTest.getConnector;
import static org.junit.Assert.assertEquals;

public class AccumuloCustomMetricStoreTest {

    private static void checkCustom(Iterable<CustomMetric<Long>> actual, int expectedNum, Long expectedVal) {
        List<CustomMetric<Long>> actualList = newArrayList(actual);

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

    @Test
    public void testStoreAndQuery() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);

        Iterable<CustomMetric<Long>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", SummingFunction.class,  MetricTimeUnit.MINUTES, new Authorizations());

        checkCustom(actual, 60, 1L);
    }

    @Test
    public void testQueryAggregation() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<CustomMetric<Long>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", SummingFunction.class, MetricTimeUnit.MINUTES, new Authorizations());

        checkCustom(actual, 60, 3L);
    }

    @Test
    public void testQueryAggregationComplex() throws Exception {
        AccumuloCustomMetricStore metricStore = new AccumuloCustomMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<CustomMetric<long[]>> actual = metricStore.queryCustom(new Date(0), new Date(), "group", "type", "name", StatsFunction.class, MetricTimeUnit.MINUTES, new Authorizations());

        checkCustomStats(actual, 60, 3);
    }
}
