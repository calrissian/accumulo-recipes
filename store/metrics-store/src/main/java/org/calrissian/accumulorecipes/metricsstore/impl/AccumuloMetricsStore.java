package org.calrissian.accumulorecipes.metricsstore.impl;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.MetricsStore;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

public class AccumuloMetricsStore implements MetricsStore {

    @Override
    public Iterator<MetricUnit> query(Date start, Date end, String group, String type, String name, String metricType, Authorizations auths) {
        return null;
    }

    @Override
    public void put(Collection<MetricUnit> metrics) {

    }

    @Override
    public void shutdown() {

    }
}
