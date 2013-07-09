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
