package org.calrissian.accumulorecipes.featurestore.support.config;

import org.apache.accumulo.core.data.Value;
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

        MetricFeature feature = new MetricFeature(currentTimeMillis(), "group", "type", "name", "vis", 1);
        Value value = new MetricFeatureConfig().buildValue(feature);

        assertEquals("1,1,1,1,1", new String(value.get()));
    }
}
