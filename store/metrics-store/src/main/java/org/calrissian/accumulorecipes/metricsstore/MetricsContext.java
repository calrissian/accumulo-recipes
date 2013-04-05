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
package org.calrissian.accumulorecipes.metricsstore;


import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.normalizer.CounterMetricNormalizer;
import org.calrissian.accumulorecipes.metricsstore.normalizer.StatsMetricNormalizer;
import org.calrissian.accumulorecipes.metricsstore.normalizer.MetricNormalizer;

import java.util.*;

public class MetricsContext {

    protected static MetricsContext instance;

    public static synchronized MetricsContext getInstance() {

        if(instance == null) {
            instance = new MetricsContext();
        }

        return instance;
    }

    Map<String, MetricNormalizer> normalizersByName = new HashMap<String, MetricNormalizer>();
    Map<Class<? extends Metric>, MetricNormalizer> normalizersByClass = new HashMap<Class<? extends Metric>, MetricNormalizer>();

    public void addNormalizer(Collection<? extends MetricNormalizer> metricTypes) {

        for(MetricNormalizer metric : metricTypes) {

            this.normalizersByName.put(metric.name(), metric);
            this.normalizersByClass.put(metric.normalizes(), metric);
        }
    }

    public MetricsContext() {

        addNormalizer(Arrays.asList(new MetricNormalizer[] {new StatsMetricNormalizer(),
                new CounterMetricNormalizer()}));
    }

    public MetricNormalizer getNormalizer(String name) {

        return normalizersByName.get(name);
    }

    public MetricNormalizer getNormalizer(Class<? extends Metric> clazz) {
        return normalizersByClass.get(clazz);
    }

    public Collection<MetricNormalizer> normalizers() {
        return normalizersByName.values();
    }
}
