package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import static java.lang.Long.parseLong;

/**
 * Custom metric function that calculates the sum of the values.
 */
public class SummingFunction implements MetricFunction<Long> {
    @Override
    public Long intitialValue() {
        return 0L;
    }

    @Override
    public Long update(Long orig, long value) {
        return orig + value;
    }

    @Override
    public Long merge(Long orig, Long value) {
        return orig + value;
    }

    @Override
    public String serialize(Long value) {
        return Long.toString(value);
    }

    @Override
    public Long deserialize(String data) {
        return parseLong(data);
    }
}
