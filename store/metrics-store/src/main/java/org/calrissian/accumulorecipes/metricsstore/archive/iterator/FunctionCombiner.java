package org.calrissian.accumulorecipes.metricsstore.archive.iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class FunctionCombiner extends Combiner {
    private static final Logger logger = LoggerFactory.getLogger(FunctionCombiner.class);

    public static final String FUNCTION_CLASS = "functionClass";
    private Function<Iterator<byte[]>, byte[]> function;
    private final FunctionValueToBytes functionValueToBytes = new FunctionValueToBytes();

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (!options.containsKey(FUNCTION_CLASS))
            throw new IllegalArgumentException("Property expected: " + FUNCTION_CLASS);

        try {
            function = (Function<Iterator<byte[]>, byte[]>)
                    Thread.currentThread().getContextClassLoader().loadClass((options.get(FUNCTION_CLASS))).newInstance();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setFunctionClass(IteratorSetting is, Class<? extends Function<Iterator<byte[]>, byte[]>> functionClazz) {
        is.addOption(FUNCTION_CLASS, functionClazz.getName());
    }

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        return new Value(function.apply(Iterators.transform(iter, functionValueToBytes)));
    }

    private class FunctionValueToBytes implements Function<Value, byte[]> {

        @Override
        public byte[] apply(Value value) {
            return value.get();
        }
    }

}
