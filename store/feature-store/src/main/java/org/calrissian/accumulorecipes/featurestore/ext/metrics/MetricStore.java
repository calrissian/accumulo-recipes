package org.calrissian.accumulorecipes.featurestore.ext.metrics;


import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.MetricTimeUnit;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.mango.collect.CloseableIterable;

import java.util.Date;

public interface MetricStore {

    void save(Iterable<? extends MetricFeature> features);

    /**
     * Query metrics back from the store.
     */
    CloseableIterable<MetricFeature> query(Date start, Date end, String group, String type,
                                          String name, MetricTimeUnit timeUnit, Auths auths);

}
