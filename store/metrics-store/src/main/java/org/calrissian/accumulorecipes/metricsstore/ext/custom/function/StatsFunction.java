package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Custom metric function that calculates the min, max, sum, and count of all the values (in that order).
 */
public class StatsFunction implements MetricFunction<long[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] intitialValue() {
        return new long[]{
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                0,
                0
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] update(long[] orig, long value) {
        return new long[]{
                min(orig[0], value),
                max(orig[1], value),
                orig[2] + value,
                orig[3] + 1
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] merge(long[] orig, long[] value) {
        return new long[]{
                min(orig[0], value[0]),
                max(orig[1], value[1]),
                orig[2] + value[2],
                orig[3] + value[3]
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serialize(long[] value) {
        return Long.toString(value[0]) + "," + Long.toString(value[1]) + "," + Long.toString(value[2]) + "," + Long.toString(value[3]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] deserialize(String data) {
        String[] individual = data.split(",");

        if (individual.length != 4)
            throw new IllegalStateException("Invalid number of elements in combiner function");

        long[] retVal = new long[4];
        for (int i = 0;i < retVal.length;i++)
            retVal[i] = Long.parseLong(individual[i]);
        return retVal;
    }
}
