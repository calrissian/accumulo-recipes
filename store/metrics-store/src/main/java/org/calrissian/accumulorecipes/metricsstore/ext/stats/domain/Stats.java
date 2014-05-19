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
package org.calrissian.accumulorecipes.metricsstore.ext.stats.domain;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.sqrt;

/**
 * Data object to hold the value of statistical metrics.
 */
public class Stats {
    private final long timestamp;
    private final String group;
    private final String type;
    private final String name;
    private final String visibility;
    private final long min;
    private final long max;
    private final long sum;
    private final long count;
    private final long sumSquare;


    public Stats(long timestamp, String group, String type, String name, String visibility,
                 long min, long max, long sum, long count, long sumSquare) {
        checkNotNull(group);
        checkNotNull(type);
        checkNotNull(name);
        checkNotNull(visibility);

        this.timestamp = timestamp;
        this.group = group;
        this.type = type;
        this.name = name;
        this.visibility = visibility;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.sumSquare = sumSquare;
    }

    /**
     * The normalized timestamp representing the beginning time for the metric
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * The group for the metric
     */
    public String getGroup() {
        return group;
    }

    /**
     * The type for the metric
     */
    public String getType() {
        return type;
    }

    /**
     * The name for the metric
     */
    public String getName() {
        return name;
    }

    /**
     * The aggregated visibility of the metric
     */
    public String getVisibility() {
        return visibility;
    }

    /**
     * The min value encountered for the time range
     */
    public long getMin() {
        return min;
    }

    /**
     * The max value encountered for the time range
     */
    public long getMax() {
        return max;
    }

    /**
     * The sum of the values encountered for the time range
     */
    public long getSum() {
        return sum;
    }

    /**
     * The number of values encountered for the time range
     */
    public long getCount() {
        return count;
    }

    /**
     * The sum of the squares for the values encountered for the time range
     */
    public long getSumSquare() {
        return sumSquare;
    }

    /**
     * The mean/average of the values encountered for the time range
     */
    public double getMean() {
        if (count < 1)
            return 0;

        return ((double) sum) / count;
    }

    /**
     * The population variance for the values encountered for the time range
     */
    public double getVariance() {
        return getVariance(false);
    }

    /**
     * The variance of the values encountered for the time range.  The asSample option
     * allows the user to get the variance of the data as if the data was a sample population.
     *
     * @see https://statistics.laerd.com/statistical-guides/measures-of-spread-standard-deviation.php
     */
    public double getVariance(boolean asSample) {
        if (asSample) {
            if (count < 2)
                return 0;

            return (double) sumSquare / (count - 1) - getMean() * (((double) sum) / (count - 1));
        } else {
            if (count < 1)
                return 0;

            return (double) sumSquare / (count) - getMean() * getMean();
        }

    }

    /**
     * The population standard deviation for the values encountered for the time range.
     */
    public double getStdDev() {
        return getStdDev(false);
    }

    /**
     * The standard deviation for the values encountered for the time range.  The asSample option
     * allows the user to get the standard deviation of the data as if the data was a sample population.
     *
     * @see https://statistics.laerd.com/statistical-guides/measures-of-spread-standard-deviation.php
     */
    public double getStdDev(boolean asSample) {
        return sqrt(getVariance(asSample));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Stats)) return false;

        Stats stats = (Stats) o;

        if (count != stats.count) return false;
        if (max != stats.max) return false;
        if (min != stats.min) return false;
        if (sum != stats.sum) return false;
        if (sumSquare != stats.sumSquare) return false;
        if (timestamp != stats.timestamp) return false;
        if (!group.equals(stats.group)) return false;
        if (!name.equals(stats.name)) return false;
        if (!type.equals(stats.type)) return false;
        if (!visibility.equals(stats.visibility)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + group.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + visibility.hashCode();
        result = 31 * result + (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        result = 31 * result + (int) (sum ^ (sum >>> 32));
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (int) (sumSquare ^ (sumSquare >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Stats{" +
                "timestamp=" + timestamp +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", visibility='" + visibility + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", sumSquare=" + sumSquare +
                '}';
    }
}
