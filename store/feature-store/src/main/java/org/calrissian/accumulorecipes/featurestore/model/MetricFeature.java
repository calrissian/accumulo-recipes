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
package org.calrissian.accumulorecipes.featurestore.model;

import org.calrissian.accumulorecipes.featurestore.FeatureStore;

import java.io.DataInput;
import java.io.IOException;

/**
 * A metric feature combines statistical summary information in a feature vector with the basic identifying information
 * required to model a feature in the {@link FeatureStore}
 */
public class MetricFeature extends Feature<Metric> {

    public MetricFeature(long timestamp, String group, String type, String name, String visibility, Metric vector) {
        super(timestamp, group, type, name, visibility, vector);
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
