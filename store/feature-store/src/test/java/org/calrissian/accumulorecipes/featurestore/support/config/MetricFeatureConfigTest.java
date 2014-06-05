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
package org.calrissian.accumulorecipes.featurestore.support.config;

import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.featurestore.model.Metric;
import org.calrissian.accumulorecipes.featurestore.model.MetricFeature;
import org.junit.Test;

import java.math.BigInteger;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

public class MetricFeatureConfigTest {

    @Test
    public void testBuildFeatureFromValue() {

        Value value = new Value("1,2,3,4,5".getBytes());

        long timestamp = currentTimeMillis();
        String group = "group";
        String type = "type";
        String name = "name";
        String vis = "vis";

        MetricFeature feature = new MetricFeatureConfig().buildFeatureFromValue(
                timestamp,
                group,
                type,
                name,
                vis,
                value
        );

        assertEquals(group, feature.getGroup());
        assertEquals(timestamp, feature.getTimestamp());
        assertEquals(type, feature.getType());
        assertEquals(name, feature.getName());
        assertEquals(vis, feature.getVisibility());

        assertEquals(1, feature.getVector().getMin());
        assertEquals(2, feature.getVector().getMax());
        assertEquals(3, feature.getVector().getSum());
        assertEquals(4, feature.getVector().getCount());
        assertEquals(BigInteger.valueOf(5), feature.getVector().getSumSquare());
    }

    @Test
    public void testBuildValueFromFeature() {

        long currentTime = currentTimeMillis();

        MetricFeature feature = new MetricFeature(currentTimeMillis(), "group", "type", "name", "vis", new Metric(1));
        Value value = new MetricFeatureConfig().buildValue(feature);

        assertEquals("1,1,1,1,1", new String(value.get()));
    }
}
