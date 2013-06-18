package org.calrissian.accumulorecipes.metricsstore.ext.custom.iterator;


import com.google.common.base.Function;
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
                    Thread.currentThread().getContextClassLoader().loadClass((options.get(FUNCTION_CLASS))).newInstance();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void setFunctionClass(IteratorSetting is, Class<? extends MetricFunction<T>> functionClazz) {
        is.addOption(FUNCTION_CLASS, functionClazz.getName());
    }

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {

        Object retVal = function.intitialValue();

        while (iter.hasNext()) {
            String data = iter.next().toString();
            if (data.length() > 0) {

                //Value is either Long or serialized with Prefix.
                if (data.startsWith(PREFIX)) {
                    retVal = function.merge(retVal, function.deserialize(data.substring(1)));
                } else {
                    retVal = function.update(retVal, Long.parseLong(data));
                }
            }
        }

        if (retVal == null)
            return new Value();

        return new Value((PREFIX + function.serialize(retVal)).getBytes());
    }

    private class FunctionValueToBytes implements Function<Value, byte[]> {

        @Override
        public byte[] apply(Value value) {
            return value.get();
        }
    }
}
