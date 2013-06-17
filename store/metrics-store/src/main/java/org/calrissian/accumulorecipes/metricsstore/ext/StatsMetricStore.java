package org.calrissian.accumulorecipes.metricsstore.ext;

import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.domain.MetricStats;

import java.util.Collection;
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
    Iterable<MetricStats> queryStats(Date start, Date end, String group, String type, String name,
                           MetricTimeUnit timeUnit, Collection<String> auths);

}
