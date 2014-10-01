/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.commons.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.support.qfd.GlobalIndexValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class GlobalIndexCombinerTest {

    /**
     * Tests that the cardinalities get summed and the max expiration is used
     */
    @Test
    public void testWithExpiration() {

        GlobalIndexCombiner combiner = new GlobalIndexCombiner();

        Collection<Value> values = new ArrayList<Value>();

        GlobalIndexValue value = new GlobalIndexValue(1, 2);
        GlobalIndexValue value2 = new GlobalIndexValue(1, 2);
        GlobalIndexValue value3 = new GlobalIndexValue(1, 50);
        GlobalIndexValue value4 = new GlobalIndexValue(1, 2);
        GlobalIndexValue value5 = new GlobalIndexValue(1, 2);

        values.add(value.toValue());
        values.add(value2.toValue());
        values.add(value3.toValue());
        values.add(value4.toValue());
        values.add(value5.toValue());

        Value actualVal = combiner.reduce(new Key(), values.iterator());

        GlobalIndexValue actualGiv = new GlobalIndexValue(actualVal);
        assertEquals(5, actualGiv.getCardinatlity());
        assertEquals(50, actualGiv.getExpiration());
    }

    /**
     * Tests that the cardinalities get summed and no expiration is used
     */
    @Test
    public void testWithNoExpiration() {

        GlobalIndexCombiner combiner = new GlobalIndexCombiner();

        Collection<Value> values = new ArrayList<Value>();

        GlobalIndexValue value = new GlobalIndexValue(1, 2);
        GlobalIndexValue value2 = new GlobalIndexValue(1, 2);
        GlobalIndexValue value3 = new GlobalIndexValue(1, -1);
        GlobalIndexValue value4 = new GlobalIndexValue(1, 2);
        GlobalIndexValue value5 = new GlobalIndexValue(1, 2);

        values.add(value.toValue());
        values.add(value2.toValue());
        values.add(value3.toValue());
        values.add(value4.toValue());
        values.add(value5.toValue());

        Value actualVal = combiner.reduce(new Key(), values.iterator());

        GlobalIndexValue actualGiv = new GlobalIndexValue(actualVal);
        assertEquals(5, actualGiv.getCardinatlity());
        assertEquals(-1, actualGiv.getExpiration());
    }
}
