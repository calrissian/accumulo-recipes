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
package org.calrissian.accumulorecipes.metricsstore.ext.stats;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.stats.domain.Stats;

import java.util.Date;

/**
 * An extended metric services which provides additional statistics on data in the store such as min, max, sum, and count.
 */
public interface StatsMetricStore extends MetricStore{

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
    Iterable<Stats> queryStats(Date start, Date end, String group, String type, String name,
                           MetricTimeUnit timeUnit, Authorizations auths);

}
