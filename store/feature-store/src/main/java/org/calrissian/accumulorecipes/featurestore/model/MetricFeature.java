package org.calrissian.accumulorecipes.featurestore.model;

import org.calrissian.accumulorecipes.featurestore.FeatureStore;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigInteger;

/**
 * A metric feature combines statistical summary information in a feature vector with the basic identifying information
 * required to model a feature in the {@link FeatureStore}
 */
public class MetricFeature extends Feature<Metric> {

    public MetricFeature(long timestamp, String group, String type, String name, String visibility, Metric vector) {
        super(timestamp, group, type, name, visibility, vector);
    }

    public MetricFeature(long timestamp, String group, String type, String name, String visibility, long metricValue) {
        super(timestamp, group, type, name, visibility, new Metric(1,1,1,1, BigInteger.valueOf(1)));
    }

    @Override
    protected Metric buildVector(DataInput input) {
        try {
            Metric vector = new Metric();
            vector.readFields(input);
            return vector;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
