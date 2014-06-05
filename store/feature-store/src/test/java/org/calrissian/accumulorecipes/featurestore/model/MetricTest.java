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
