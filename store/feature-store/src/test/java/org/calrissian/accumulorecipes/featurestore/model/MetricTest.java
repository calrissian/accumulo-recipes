package org.calrissian.accumulorecipes.featurestore.model;

import org.junit.Test;

import java.math.BigInteger;

import static junit.framework.TestCase.assertEquals;

public class MetricTest {

    @Test
    public void testVariance() {

        Metric metric = new Metric(1);
        assertEquals(0.0, metric.getVariance());

        metric = new Metric(1, 1, 1, 5, BigInteger.valueOf(1));
        assertEquals(0.16, metric.getVariance());

        metric = new Metric(1, 1, 5, 5, BigInteger.valueOf(5));
        assertEquals(0.0, metric.getVariance());
        
    }
}
