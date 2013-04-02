package org.calrissian.accumulorecipes.eventstore.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import java.io.IOException;

public class EventIterator extends WrappingIterator {

    protected SortedKeyValueIterator<Key,Value> sourceItr;

    public void init(SortedKeyValueIterator<Key,Value> source, java.util.Map<String,String> options,
                     IteratorEnvironment env) throws IOException {

        super.init(source, options, env);
        sourceItr = source.deepCopy(env);
    }

    @Override
    public Value getTopValue() {

        if(hasTop()) {

            return IteratorUtils.retrieveFullEvent(getTopKey(), sourceItr);
        }

        return new Value("".getBytes());
    }
}
