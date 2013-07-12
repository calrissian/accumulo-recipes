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
package org.calrissian.accumulorecipes.metricsstore.ext.custom.iterator;


import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.calrissian.accumulorecipes.metricsstore.ext.custom.function.MetricFunction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static java.lang.Long.parseLong;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.copyOfRange;
import static org.calrissian.accumulorecipes.metricsstore.support.Constants.DELIM;

/**
 * Accumulo Combiner class that handles the generic logic for enabling custom function aggregation.
 */
public class FunctionCombiner extends Combiner {
    private static final String PREFIX = DELIM;

    public static final String FUNCTION_CLASS = "functionClass";

    private MetricFunction function;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (!options.containsKey(FUNCTION_CLASS))
            throw new IllegalArgumentException("Property expected: " + FUNCTION_CLASS);

        try {
            function = (MetricFunction)
                    currentThread().getContextClassLoader().loadClass((options.get(FUNCTION_CLASS))).newInstance();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void setFunctionClass(IteratorSetting is, Class<? extends MetricFunction<T>> functionClazz) {
        is.addOption(FUNCTION_CLASS, functionClazz.getName());
    }

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        if (!iter.hasNext())
            return new Value();

        function.reset();

        while (iter.hasNext()) {
            byte[] data = iter.next().get();
            if (data.length > 0) {
                //Value is either Long or serialized with Prefix.
                if (data[0] == 0)
                    function.merge(function.deserialize(copyOfRange(data, 1, data.length)));
                else
                    function.update(parseLong(new String(data)));

            }
        }
        byte[] data = function.serialize();
        byte[] prefixed = new byte[data.length + 1];
        prefixed[0] = 0;
        for (int i = 0;i< data.length; i++)
            prefixed[i + 1] = data[i];

        return new Value(prefixed);
    }
}
