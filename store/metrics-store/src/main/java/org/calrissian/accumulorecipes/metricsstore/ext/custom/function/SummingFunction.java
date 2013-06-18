package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import static java.lang.Long.parseLong;

/**
 * Custom metric function that calculates the sum of the values.
 */
public class SummingFunction implements MetricFunction<Long> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Long intitialValue() {
        return 0L;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long update(Long orig, long value) {
        return orig + value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long merge(Long orig, Long value) {
        return orig + value;
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
