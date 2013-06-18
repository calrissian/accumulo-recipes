package org.calrissian.accumulorecipes.metricsstore.ext;

import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.domain.Stats;

import java.util.Date;

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
