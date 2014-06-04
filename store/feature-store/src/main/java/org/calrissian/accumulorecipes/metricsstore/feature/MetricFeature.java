package org.calrissian.accumulorecipes.metricsstore.feature;

import org.calrissian.accumulorecipes.metricsstore.feature.vector.MetricFeatureVector;

public class MetricFeature extends BaseFeature<MetricFeatureVector> {

    public MetricFeature(long timestamp, String group, String type, String name, String visibility, MetricFeatureVector vector) {
        super(timestamp, group, type, name, visibility, vector);
    }
}
