package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl.function;

import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MaxFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;

public class MaxFunctionTest extends BaseFunctionTest<Long>{
    @Override
    public MetricFunction<Long> getFunction() {
        return new MaxFunction();
    }

    @Override
    public boolean assertEquals(Long value1, Long value2) {
        return value1.equals(value2);
    }
}
