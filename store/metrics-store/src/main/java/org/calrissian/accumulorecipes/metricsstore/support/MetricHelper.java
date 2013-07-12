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
package org.calrissian.accumulorecipes.metricsstore.support;


import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;

import static com.google.common.collect.Iterables.size;
import static com.google.common.math.LongMath.checkedAdd;

public class MetricHelper {

    public static long count(Iterable<Metric> metrics) {
        return size(metrics);
    }

    public static long sum(Iterable<Metric> metrics) {
        long sum = 0;
        for (Metric metric : metrics)
            checkedAdd(sum, metric.getValue());

        return sum;
    }

    public static double average(Iterable<Metric> metrics) {
        long sum = 0;
        long count = 0;

        for (Metric metric : metrics) {
            checkedAdd(sum, metric.getValue());
            checkedAdd(count, 1);
        }

        return sum / (double)count;
    }

    public static SummaryStatistics summaryStatistics(Iterable<Metric> metrics) {
        SummaryStatistics stats = new SummaryStatistics();

        for (Metric metric : metrics)
            stats.addValue(metric.getValue());

        return stats;
    }

    public static NormalDistribution normalDistribution(Iterable<Metric> metrics) {
        SummaryStatistics stats = summaryStatistics(metrics);
        return new NormalDistribution(stats.getMean(), stats.getStandardDeviation());
    }

}
