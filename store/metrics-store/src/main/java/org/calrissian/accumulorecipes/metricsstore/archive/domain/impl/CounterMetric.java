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
package org.calrissian.accumulorecipes.metricsstore.archive.domain.impl;

import org.calrissian.accumulorecipes.metricsstore.archive.domain.Metric;

public class CounterMetric implements Metric {

    Long count;

    public CounterMetric(Long count) {
        this.count = count;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CounterMetric)) return false;

        CounterMetric that = (CounterMetric) o;

        if (count != null ? !count.equals(that.count) : that.count != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return count != null ? count.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "CounterMetric{" +
                "count=" + count +
                '}';
    }
}
