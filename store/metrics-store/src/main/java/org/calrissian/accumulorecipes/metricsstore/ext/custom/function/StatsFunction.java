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
package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import static java.lang.Long.parseLong;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.join;

/**
 * Custom metric function that calculates the min, max, sum, and count of all the values (in that order).
 */
public class StatsFunction implements MetricFunction<long[]> {

    long[] stats;

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        stats = new long[]{
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                0,
                0,
                0
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(long value) {
        stats[0] = min(stats[0], value);
        stats[1] = max(stats[1], value);
        stats[2] = stats[2] + value;
        stats[3] = stats[3] + 1;
        stats[4] = stats[4] + (value * value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void merge(long[] value) {
        stats[0] = min(stats[0], value[0]);
        stats[1] = max(stats[1], value[1]);
        stats[2] = stats[2] + value[2];
        stats[3] = stats[3] + value[3];
        stats[4] = stats[4] + value[4];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize() {
        return join(asList(Long.toString(stats[0]), Long.toString(stats[1]), Long.toString(stats[2]), Long.toString(stats[3]), Long.toString(stats[4])), ",").getBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] deserialize(byte[] data) {
        String[] individual = new String(data).split(",");

        if (individual.length != 5)
            throw new IllegalStateException("Invalid number of elements in combiner function");

        long[] retVal = new long[5];
        for (int i = 0; i < retVal.length; i++)
            retVal[i] = parseLong(individual[i]);
        return retVal;
    }
}
