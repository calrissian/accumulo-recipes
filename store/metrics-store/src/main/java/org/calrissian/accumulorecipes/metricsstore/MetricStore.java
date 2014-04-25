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


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

public interface MetricStore {

    /**
     * Save a set of metrics to the store
     * @param metricData
     */
    void save(Iterable<Metric> metricData);

    /**
     * Query metrics back from the store.
     * @param start
     * @param end
     * @param group
     * @param type
     * @param name
     * @param timeUnit
     * @param auths
     * @return
     */
    CloseableIterable<Metric> query(Date start, Date end, String group, String type, String name,
                                    MetricTimeUnit timeUnit, Auths auths);

}
