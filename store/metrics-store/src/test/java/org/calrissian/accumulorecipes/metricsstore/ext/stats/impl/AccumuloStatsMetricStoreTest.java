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
package org.calrissian.accumulorecipes.metricsstore.ext.stats.impl;

import com.google.common.base.Function;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.domain.Stats;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Random;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static java.lang.Math.sqrt;
import static java.util.Arrays.asList;
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
            assertEquals(1.0, stat.getMean(), Double.MIN_NORMAL);
            assertEquals(0.0, stat.getStdDev(true), Double.MIN_NORMAL);
        }
    }

    @Test
    public void testStoreAndQuery() throws Exception {
        AccumuloStatsMetricStore metricStore = new AccumuloStatsMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);

        Iterable<Metric> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());

        checkMetrics(actual, 60, 1);

        Iterable<Stats> stats = metricStore.queryStats(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());

        checkStats(stats, 60, 1);
    }

    @Test
    public void testQueryAggregation() throws Exception {
        AccumuloStatsMetricStore metricStore = new AccumuloStatsMetricStore(getConnector());

        Iterable<Metric> testData = generateTestData(MetricTimeUnit.MINUTES, 60);

        metricStore.save(testData);
        metricStore.save(testData);
        metricStore.save(testData);

        Iterable<Metric> actual = metricStore.query(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());

        checkMetrics(actual, 60, 3);

        Iterable<Stats> stats = metricStore.queryStats(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths());

        checkStats(stats, 60, 3);
    }

    @Test
    public void testStatisticAccuracy() throws Exception{
        AccumuloStatsMetricStore metricStore = new AccumuloStatsMetricStore(getConnector());

        Random random = new Random();

        List<Long> sampleData = asList(
                (long)random.nextInt(10000),
                (long)random.nextInt(10000),
                (long)random.nextInt(10000),
                (long)random.nextInt(10000),
                (long)random.nextInt(10000)
        );

        //use commons math as a
        SummaryStatistics sumStats = new SummaryStatistics();
        for (long num :sampleData)
            sumStats.addValue(num);

        final long timestamp = System.currentTimeMillis();
        Iterable<Metric> testData = transform(sampleData, new Function<Long, Metric>() {
            @Override
            public Metric apply(Long num) {
                return new Metric(timestamp, "group", "type", "name", "", num);
            }
        });

        metricStore.save(testData);

        List<Stats> stats = newArrayList(metricStore.queryStats(new Date(0), new Date(), "group", "type", "name", MetricTimeUnit.MINUTES, new Auths()));

        assertEquals(1, stats.size());
        Stats stat = stats.get(0);

        assertEquals(sumStats.getMin(), stat.getMin(), Double.MIN_NORMAL);
        assertEquals(sumStats.getMax(), stat.getMax(), Double.MIN_NORMAL);
        assertEquals(sumStats.getSum(), stat.getSum(), Double.MIN_NORMAL);
        assertEquals(sumStats.getN(), stat.getCount(), Double.MIN_NORMAL);
        assertEquals(sumStats.getMean(), stat.getMean(), Double.MIN_NORMAL);
        assertEquals(sumStats.getPopulationVariance(), stat.getVariance(), 0.00000001);
        assertEquals(sumStats.getVariance(), stat.getVariance(true), 0.00000001);
        assertEquals(sqrt(sumStats.getPopulationVariance()), stat.getStdDev(), 0.00000001);
        assertEquals(sumStats.getStandardDeviation(), stat.getStdDev(true), 0.00000001);
    }
}
