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
package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.join;

public class NormalDistFunction implements MetricFunction<double[]> {

    SummaryStatistics stats;

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        stats = new SummaryStatistics();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(long value) {
        stats.addValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void merge(double[] value) {
        //This is ok, this is only used when this is configured on a compaction iterator.
        throw new UnsupportedOperationException("Can't merge data for normal dist");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serialize() {
        return join(
                asList(Double.toString(stats.getMax()), Double.toString(stats.getMin()), Double.toString(stats.getMean()), Double.toString(stats.getVariance()), Double.toString(stats.getN())),
                ",");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double[] deserialize(String data) {
        return new double[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
