/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        AccumuloFeatureConfig featureConfig = classToTransform.get(clazz);
        if(featureConfig == null) {
            for(Map.Entry<Class, AccumuloFeatureConfig> clazzes : classToTransform.entrySet()) {
                if(clazzes.getKey().isAssignableFrom(clazz)) {
                    featureConfig = clazzes.getValue();
                    classToTransform.put(clazz, featureConfig);
                }
            }
        }
        return featureConfig;
    }

    public Iterable<AccumuloFeatureConfig> getConfigs() {
        return classToTransform.values();
    }
}
