package org.calrissian.accumulorecipes.metricsstore;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricUnit;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * A metrics store allows the storage of numerically quantified
 */
public interface MetricsStore {

    Iterator<MetricUnit> query(Date start, Date end, String group, String type, String name, String metricType,
                               Authorizations auths);

    void put(Collection<MetricUnit> metrics);

    void shutdown();
}
