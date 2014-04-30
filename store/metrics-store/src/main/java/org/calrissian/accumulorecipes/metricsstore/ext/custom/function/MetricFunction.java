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


/**
 * Custom function for defining the aggregation behavior of metrics for a criteria.
 * @param <T>
 */
public interface MetricFunction<T> {

    void reset();

    /**
     * Updates the original metric with the given value.
     * @param value
     */
    void update(long value);

    /**
     * Merges the two metrics together.
     *
     * TODO With no iterator settings specified during compactions, this should never happen and we can use
     * more complex iterable math functions.
     *
     * @param value
     */
    void merge(T value);

    /**
     * Serialize the given metric
     * @return
     */
    byte[] serialize();

    /**
     * Deserialize the given metric.
     * @param data
     * @return
     */
    T deserialize(byte[] data);

}
