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
package org.calrissian.accumulorecipes.featurestore.ext.metrics;

import java.util.Date;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.mango.collect.CloseableIterable;

/**
 * A metrics store API for persisting and querying statistical summaries of things. These statistical summaries
 * are min, max, sum, count, and sumSquare (for std deviation).
 */
public interface MetricStore {

    /**
     * Saves a bunch of metrics to the metrics store
     */
    void save(Iterable<? extends MetricFeature> metrics);

    /**
     * Query metrics back from the store.
     */
    CloseableIterable<MetricFeature> query(Date start, Date end, String group, String type,
                                          String name, TimeUnit timeUnit, Auths auths);

    void flush() throws Exception;

}
