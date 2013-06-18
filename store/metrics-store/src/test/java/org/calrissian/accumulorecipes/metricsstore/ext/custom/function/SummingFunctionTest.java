package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;

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
