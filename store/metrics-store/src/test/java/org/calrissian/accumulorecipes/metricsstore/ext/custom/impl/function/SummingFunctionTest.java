package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl.function;

import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.SummingFunction;

public class SummingFunctionTest extends BaseFunctionTest<Long>{
    @Override
    public MetricFunction<Long> getFunction() {
        return new SummingFunction();
    }

    @Override
    public boolean assertEquals(Long value1, Long value2) {
        return value1.equals(value2);
    }
}
