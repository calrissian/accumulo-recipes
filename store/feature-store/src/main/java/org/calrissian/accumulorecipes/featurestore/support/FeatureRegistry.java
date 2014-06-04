package org.calrissian.accumulorecipes.featurestore.support;

import org.calrissian.accumulorecipes.featurestore.feature.BaseFeature;
import org.calrissian.accumulorecipes.featurestore.feature.transform.AccumuloFeatureConfig;
import org.calrissian.accumulorecipes.featurestore.feature.transform.MetricFeatureTransform;

import java.util.HashMap;
import java.util.Map;

public class FeatureRegistry {

    private Map<Class, AccumuloFeatureConfig> classToTransform = new HashMap<Class, AccumuloFeatureConfig>();
    private Map<String, AccumuloFeatureConfig> nameToTransform = new HashMap<String, AccumuloFeatureConfig>();

    public FeatureRegistry(AccumuloFeatureConfig... transforms) {

        for(AccumuloFeatureConfig featureTransform : transforms) {
            classToTransform.put(featureTransform.transforms(), featureTransform);
            nameToTransform.put(featureTransform.featureName(), featureTransform);
        }
    }

    public AccumuloFeatureConfig transformForClass(Class<? extends BaseFeature> clazz) {
        return classToTransform.get(clazz);
    }

    public AccumuloFeatureConfig transformForName(String featureName) {
        return nameToTransform.get(featureName);
    }

    public static final FeatureRegistry BASE_FEATURES = new FeatureRegistry(new MetricFeatureTransform());

    public Iterable<AccumuloFeatureConfig> getConfigs() {
        return classToTransform.values();
    }

}
