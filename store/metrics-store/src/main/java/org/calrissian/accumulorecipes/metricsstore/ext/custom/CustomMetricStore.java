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
package org.calrissian.accumulorecipes.metricsstore.ext.custom;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

/**
 * Allows a caller to define a custom metric function at criteria time for aggregating data.
 */
public interface CustomMetricStore extends MetricStore {

    /**
     * Query metrics back from the store.
     *
     * @param start
     * @param end
     * @param group
     * @param type
     * @param name
     * @param function
     * @param timeUnit
     * @param auths
     * @param <T>
     * @return
     */
    <T> CloseableIterable<CustomMetric<T>> queryCustom(Date start, Date end, String group, String type, String name,
                                                       Class<? extends MetricFunction<T>> function, MetricTimeUnit timeUnit, Auths auths) throws IllegalAccessException, InstantiationException;

}
