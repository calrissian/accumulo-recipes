/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.metricsstore;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricType;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * A metrics store allows the storage of numerically quantified
 */
public interface MetricsStore {

    /**
     * Query the metrics store for a given metric for a given time range
     * @param start
     * @param end
     * @param group
     * @param type
     * @param name
     * @param metricType
     * @param auths
     * @return
     */
    Iterator<MetricUnit> query(Date start, Date end, String group, String type, String name, MetricType metricType,
                               MetricTimeUnit metricTimeUnit, Authorizations auths);

    /**
     * Put a collection of metric units into the metrics store
     * @param metrics
     */
    void put(Collection<MetricUnit> metrics);

    /**
     * Free up resources upon shutdown
     */
    void shutdown();
}
