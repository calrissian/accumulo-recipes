package org.calrissian.accumulorecipes.featurestore.support;

import org.calrissian.accumulorecipes.featurestore.model.Metric;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.calrissian.accumulorecipes.featurestore.support.config.MetricFeatureConfig;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class FeatureRegistryTest {

    @Test
    public void testMyTestFeatureReturns() {

        FeatureRegistry registry = new FeatureRegistry(new MetricFeatureConfig());
        assertNotNull(registry.transformForClass(MyTestFeature.class));

    }

    private class MyTestFeature extends MetricFeature {
        public MyTestFeature(long timestamp, String group, String type, String name, String visibility, Metric vector) {
            super(timestamp, group, type, name, visibility, vector);
        }
    }
}

