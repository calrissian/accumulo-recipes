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
