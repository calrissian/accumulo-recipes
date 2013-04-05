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
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.calrissian.accumulorecipes.metricsstore.domain.impl.CounterMetric;

public class CounterMetricNormalizer implements MetricNormalizer<CounterMetric> {

    @Override
    public String name() {
        return "COUNTER";
    }

    @Override
    public Value getValue(CounterMetric metric) {
        return new Value(String.format("%d", metric.getCount()).getBytes());
    }

    @Override
    public CounterMetric getMetric(Value value) {
        return new CounterMetric(Long.parseLong(new String(value.get())));
    }

    @Override
    public Class<CounterMetric> normalizes() {
        return CounterMetric.class;
    }

    @Override
    public Class<? extends Combiner> combinerClass() {
        return SummingCombiner.class;
    }

    @Override
    public void setSpecificIteratorOptions(IteratorSetting is) {

        SummingCombiner.setEncodingType(is, SummingCombiner.Type.STRING);
    }
}
