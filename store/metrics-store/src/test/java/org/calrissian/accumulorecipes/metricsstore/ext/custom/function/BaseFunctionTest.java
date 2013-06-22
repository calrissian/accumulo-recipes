/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import org.junit.Test;

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
