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
package org.calrissian.accumulorecipes.featurestore.ext.metrics.impl;

import org.apache.accumulo.core.client.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.ext.metrics.MetricStore;
import org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry;
import org.calrissian.accumulorecipes.featurestore.support.config.MetricFeatureConfig;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

/**
 * The Accumulo implementation of the metrics store allows the statistical summaries to be aggregated during the
 * compaction and scan phases. Metrics gets automatically rolled up for the given time units.
 */
public class AccumuloMetricsStore implements MetricStore{

    public static final String DEFAULT_TABLE_NAME = "metrics";

    protected AccumuloFeatureStore featureStore;

    public AccumuloMetricsStore(Connector connector) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, new StoreConfig());
    }

    public AccumuloMetricsStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        featureStore = new AccumuloFeatureStore(connector, tableName, config);
        featureStore.setFeatureRegistry(new FeatureRegistry(new MetricFeatureConfig()));    // make sure the only feature type is the metrics feature type
        featureStore.initialize();
    }

    @Override
    public void save(Iterable<? extends MetricFeature> features) {
        this.featureStore.save(features);
    }

    @Override
    public CloseableIterable<MetricFeature> query(Date start, Date end, String group, String type, String name, TimeUnit timeUnit, Auths auths) {
        return featureStore.query(start, end, group, type, name, timeUnit, MetricFeature.class, auths);
    }
}
