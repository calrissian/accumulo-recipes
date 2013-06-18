package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl.function;

import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MinFunction;

public class MinFunctionTest extends BaseFunctionTest<Long>{
    @Override
    public MetricFunction<Long> getFunction() {
        return new MinFunction();
    }

    @Override
    public boolean assertEquals(Long value1, Long value2) {
        return value1.equals(value2);
    }
}