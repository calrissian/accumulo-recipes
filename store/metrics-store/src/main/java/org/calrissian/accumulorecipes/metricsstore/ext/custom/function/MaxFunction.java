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

/**
 * Custom metric function that calculates the max value.
 */
public class MaxFunction implements MetricFunction<Long> {

    long max;

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        max = Long.MIN_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(long value) {
        max = max(max, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void merge(Long value) {
        max = max(max, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serialize() {
        return Long.toString(max);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long deserialize(String data) {
        return parseLong(data);
    }
}
