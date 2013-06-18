package org.calrissian.accumulorecipes.metricsstore.ext.custom;


import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.metricsstore.MetricStore;
import org.calrissian.accumulorecipes.metricsstore.domain.MetricTimeUnit;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.domain.CustomMetric;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;

import java.util.Date;

/**
 * Allows a caller to define a custom metric function at query time for aggregating data.
 */
public interface CustomMetricStore extends MetricStore {

    /**
     * Query metrics back from the store.
     * @param start
     * @param end
     * @param group
     * @param type
     * @param name
     * @param function
     * @param timeUnit
     * @param auths
     * @param <T>
     * @return
     */
    <T> Iterable<CustomMetric<T>> queryCustom(Date start, Date end, String group, String type, String name,
                               Class<? extends MetricFunction<T>> function, MetricTimeUnit timeUnit, Authorizations auths) throws IllegalAccessException, InstantiationException;

}
