package org.calrissian.accumulorecipes.featurestore.ext.metrics.impl;

import org.apache.accumulo.core.client.*;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreConfig;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.featurestore.FeatureStore;
import org.calrissian.accumulorecipes.featurestore.ext.metrics.MetricStore;
import org.calrissian.accumulorecipes.featurestore.impl.AccumuloFeatureStore;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

public class AccumuloMetricsStore implements MetricStore{

    protected FeatureStore featureStore;

    public AccumuloMetricsStore(Connector connector) throws TableNotFoundException, AccumuloSecurityException, AccumuloException, TableExistsException {
        this.featureStore = new AccumuloFeatureStore(connector);
    }

    public AccumuloMetricsStore(Connector connector, String tableName, StoreConfig config) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.featureStore = new AccumuloFeatureStore(connector, tableName, config);
    }

    @Override
    public void save(Iterable<? extends MetricFeature> features) {
        this.featureStore.save(features);
    }

    @Override
    public CloseableIterable<MetricFeature> query(Date start, Date end, String group, String type, String name, MetricTimeUnit timeUnit, Auths auths) {
        return featureStore.query(start, end, group, type, name, timeUnit, MetricFeature.class, auths);
    }
}
