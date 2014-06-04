package org.calrissian.accumulorecipes.featurestore.feature;

import org.calrissian.accumulorecipes.featurestore.feature.vector.MetricFeatureVector;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigInteger;

public class MetricFeature extends BaseFeature<MetricFeatureVector> {

    public MetricFeature(long timestamp, String group, String type, String name, String visibility, MetricFeatureVector vector) {
        super(timestamp, group, type, name, visibility, vector);
    }

    public MetricFeature(long timestamp, String group, String type, String name, String visibility, long metricValue) {
        super(timestamp, group, type, name, visibility, new MetricFeatureVector(1,1,1,1, BigInteger.valueOf(1)));
    }

    @Override
    protected MetricFeatureVector buildVector(DataInput input) {
        try {
            MetricFeatureVector vector = new MetricFeatureVector();
            vector.readFields(input);
            return vector;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
