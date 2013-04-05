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
package org.calrissian.accumulorecipes.metricsstore.domain.impl;

import org.calrissian.accumulorecipes.metricsstore.domain.Metric;

public class StatsMetric implements Metric {

    Long min;
    Long max;
    Long count;
    Long sum;

    public StatsMetric(Long min, Long max, Long count, Long sum) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.sum = sum;
    }

    public Long getMin() {
        return min;
    }

    public Long getMax() {
        return max;
    }

    public Long getCount() {
        return count;
    }

    public Long getSum() {
        return sum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsMetric)) return false;

        StatsMetric metric = (StatsMetric) o;

        if (sum != null ? !sum.equals(metric.sum) : metric.sum != null) return false;
        if (count != null ? !count.equals(metric.count) : metric.count != null) return false;
        if (max != null ? !max.equals(metric.max) : metric.max != null) return false;
        if (min != null ? !min.equals(metric.min) : metric.min != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = min != null ? min.hashCode() : 0;
        result = 31 * result + (max != null ? max.hashCode() : 0);
        result = 31 * result + (count != null ? count.hashCode() : 0);
        result = 31 * result + (sum != null ? sum.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "StatsMetric{" +
                "min=" + min +
                ", max=" + max +
                ", count=" + count +
                ", sum=" + sum +
                '}';
    }
}