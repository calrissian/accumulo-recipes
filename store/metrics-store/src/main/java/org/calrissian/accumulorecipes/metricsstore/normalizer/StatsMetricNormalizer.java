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
package org.calrissian.accumulorecipes.metricsstore.normalizer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.calrissian.accumulorecipes.metricsstore.domain.impl.StatsMetric;
import org.calrissian.accumulorecipes.metricsstore.iterator.StatsCombiner;

public class StatsMetricNormalizer implements MetricNormalizer<StatsMetric> {

    @Override
    public String name() {
        return "STATS";
    }

    @Override
    public Value getValue(StatsMetric metric) {
        return new Value(String.format("%d,%d,%d,%d",
                metric.getMin(), metric.getMax(), metric.getCount(), metric.getSum()).getBytes());
    }

    @Override
    public StatsMetric getMetric(Value value) {

        String val[] = new String(value.get()).split(",");
        return new StatsMetric(Long.parseLong(val[0]), Long.parseLong(val[1]), Long.parseLong(val[2]),
                Long.parseLong(val[3]));
    }

    @Override
    public Class<StatsMetric> normalizes() {
        return StatsMetric.class;
    }

    @Override
    public Class<? extends Combiner> combinerClass() {
        return StatsCombiner.class;
    }

    @Override
    public void setSpecificIteratorOptions(IteratorSetting is) {}
}
