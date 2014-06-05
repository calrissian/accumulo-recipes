package org.calrissian.accumulorecipes.featurestore.support;

import org.calrissian.accumulorecipes.featurestore.model.Feature;
import org.calrissian.accumulorecipes.featurestore.support.config.AccumuloFeatureConfig;
import org.calrissian.accumulorecipes.featurestore.support.config.MetricFeatureConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds a set of feature classes and their mappings to config objects.
 */
public class FeatureRegistry {

    public static final FeatureRegistry BASE_FEATURES = new FeatureRegistry(new MetricFeatureConfig());

    private Map<Class, AccumuloFeatureConfig> classToTransform = new HashMap<Class, AccumuloFeatureConfig>();

    public FeatureRegistry(AccumuloFeatureConfig... transforms) {

        for(AccumuloFeatureConfig featureTransform : transforms)
            classToTransform.put(featureTransform.transforms(), featureTransform);
    }

    public AccumuloFeatureConfig transformForClass(Class<? extends Feature> clazz) {
        return classToTransform.get(clazz);
    }

    public Iterable<AccumuloFeatureConfig> getConfigs() {
        return classToTransform.values();
    }
}
