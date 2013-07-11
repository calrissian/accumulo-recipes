package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;

import static java.lang.Long.parseLong;

public class SumFunction implements MetricFunction<Long> {

    long sum;

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        sum = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(long value) {
        sum += value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void merge(Long value) {
        sum += value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serialize() {
        return Long.toString(sum);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long deserialize(String data) {
        return parseLong(data);
    }
}
