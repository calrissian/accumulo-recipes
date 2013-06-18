package org.calrissian.accumulorecipes.metricsstore.ext.custom.impl.function;


import org.junit.Test;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;

public abstract class BaseFunctionTest<T> {


    public abstract MetricFunction<T> getFunction();

    public abstract boolean assertEquals(T value1, T value2);

    /**
     * Verifies that the intial value produces the same result from a merge as from an update.
     */
    @Test
    public void identityTest(){
        MetricFunction<T> function = getFunction();
        T updatedValue = function.update(function.intitialValue(), 1);

        assertEquals(updatedValue, function.merge(function.intitialValue(), updatedValue));
    }

    /**
     * Tests that the merge function provides the same results as a multiple updates.
     */
    @Test
    public void associativeTest() {
        MetricFunction<T> function = getFunction();
        T update = function.update(function.intitialValue(), 1);


        T value1 = function.intitialValue();
        T value2 = function.intitialValue();
        for (int i = 0;i < 10; i++) {
            value1 = function.update(value1, 1);
            value2 = function.merge(value2, update);
        }

        assertEquals(value1, value2);
    }

    /**
     * Tests that serialization and deserialization produce the same results.
     */
    @Test
    public void serializeDeserializeTest() {
        MetricFunction<T> function = getFunction();

        T value = function.intitialValue();
        for (int i = 0;i < 10; i++)
            value = function.update(value, 1);

        assertEquals(value, function.deserialize(function.serialize(value)));
    }

}
