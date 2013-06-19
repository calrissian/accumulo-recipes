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
package org.calrissian.accumulorecipes.metricsstore.ext.stats.impl;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.domain.Stats;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.calrissian.accumulorecipes.metricsstore.impl.AccumuloMetricStoreTest.*;
import static org.junit.Assert.assertEquals;


public class AccumuloStatsMetricStoreTest {

    private static void checkStats(Iterable<Stats> actual, int expectedNum, int expectedVal) {
        List<Stats> actualList = newArrayList(actual);

        assertEquals(expectedNum, actualList.size());

        for (Stats stat : actualList) {
            assertEquals("group", stat.getGroup());
            assertEquals("type", stat.getType());
            assertEquals("name", stat.getName());
            assertEquals("", stat.getVisibility());
            assertEquals(1, stat.getMin());
            assertEquals(1, stat.getMax());
            assertEquals(expectedVal, stat.getCount());
            assertEquals(expectedVal, stat.getSum());
        }
    }

    @Test
    public void testStoreAndQuery() throws Exception {
        AccumuloStatsMetricStore metricStore = new AccumuloStatsMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);

        Iterable<Metric> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Authorizations());

        checkMetrics(actual, 60, 1);

        Iterable<Stats> stats = metricStore.queryStats(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Authorizations());

        checkStats(stats, 60, 1);
    }

    @Test
    public void testQueryAggregation() throws Exception {
        AccumuloStatsMetricStore metricStore = new AccumuloStatsMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<Metric> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Authorizations());

        checkMetrics(actual, 60, 3);

        Iterable<Stats> stats = metricStore.queryStats(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Authorizations());

        checkStats(stats, 60, 3);
    }
}
