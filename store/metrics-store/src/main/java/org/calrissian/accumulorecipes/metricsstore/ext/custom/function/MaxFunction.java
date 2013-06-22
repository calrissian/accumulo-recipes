package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import static java.lang.Long.parseLong;
import static java.lang.Math.max;

/**
 * Custom metric function that calculates the max value.
 */
public class MaxFunction implements MetricFunction<Long> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Long intitialValue() {
        return Long.MIN_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long update(Long orig, long value) {
        return max(orig, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long merge(Long orig, Long value) {
        return max(orig, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serialize(Long value) {
        return Long.toString(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long deserialize(String data) {
        return parseLong(data);
    }
}
