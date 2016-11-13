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

import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.ext.metrics.MetricStore;
import org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.accumulorecipes.featurestore.support.FeatureRegistry;
import org.calrissian.accumulorecipes.featurestore.support.config.MetricFeatureConfig;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.types.TypeRegistry;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

/**
 * The Accumulo implementation of the metrics store allows the statistical summaries to be aggregated during the
 * compaction and scan phases. Metrics gets automatically rolled up for the given time units.
 */
public class AccumuloMetricsStore implements MetricStore{

    public static final String DEFAULT_TABLE_NAME = "metrics";
    public static final StoreConfig DEFAULT_STORE_CONFIG = new StoreConfig();

    protected AccumuloFeatureStore featureStore;

    public AccumuloMetricsStore(Connector connector) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this(connector, DEFAULT_TABLE_NAME, DEFAULT_STORE_CONFIG);
    }

    public AccumuloMetricsStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        featureStore = new AccumuloFeatureStore(connector, tableName, config, new FeatureRegistry(new MetricFeatureConfig()));
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

    @Override
    public void flush() throws Exception {
        featureStore.flush();
    }

    public static class Builder {
        private final Connector connector;
        private String tableName = DEFAULT_TABLE_NAME;
        private StoreConfig config = DEFAULT_STORE_CONFIG;

        public Builder(Connector connector) {
            checkNotNull(connector);
            this.connector = connector;
        }

        public Builder setTableName(String tableName) {
            checkNotNull(tableName);
            this.tableName = tableName;
            return this;
        }

        public Builder setConfig(StoreConfig config) {
            checkNotNull(config);
            this.config = config;
            return this;
        }

        public AccumuloMetricsStore build() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
            return new AccumuloMetricsStore(connector, tableName, config);
        }

    }
}
