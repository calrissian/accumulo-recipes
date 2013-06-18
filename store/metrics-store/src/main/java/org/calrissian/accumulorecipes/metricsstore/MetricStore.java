package org.calrissian.accumulorecipes.metricsstore;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.domain.Metric;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;

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
    Iterable<Metric> query(Date start, Date end, String group, String type, String name,
                           MetricTimeUnit timeUnit, Authorizations auths);

}
