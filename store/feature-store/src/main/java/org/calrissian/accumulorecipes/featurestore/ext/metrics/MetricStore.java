package org.calrissian.accumulorecipes.featurestore.ext.metrics;

import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.TimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

/**
 * A metrics store API for persisting and querying statistical summaries of things. These statistical summaries
 * are min, max, sum, count, and sumSquare (for std deviation).
 */
public interface MetricStore {

    /**
     * Saves a bunch of metrics to the metrics store
     */
    void save(Iterable<? extends MetricFeature> metrics);

    /**
     * Query metrics back from the store.
     */
    CloseableIterable<MetricFeature> query(Date start, Date end, String group, String type,
                                          String name, TimeUnit timeUnit, Auths auths);

}
