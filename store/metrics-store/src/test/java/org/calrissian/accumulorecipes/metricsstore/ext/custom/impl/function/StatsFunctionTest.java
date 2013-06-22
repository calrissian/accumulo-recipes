package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl.function;

import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.StatsFunction;

import java.util.Arrays;


public class StatsFunctionTest extends BaseFunctionTest<long[]> {
    @Override
    public MetricFunction<long[]> getFunction() {
        return new StatsFunction();
    }

    @Override
    public boolean assertEquals(long[] value1, long[] value2) {
        return Arrays.equals(value1, value2);
    }
}
