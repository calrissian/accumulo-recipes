package org.calrissian.accumulorecipes.eventstore.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;

import java.io.IOException;

public class EventIntersectingIterator extends IntersectingIterator {

    protected SortedKeyValueIterator<Key,Value> sourceItr;

    protected Key topKey;

    public void init(SortedKeyValueIterator<Key,Value> source, java.util.Map<String,String> options, IteratorEnvironment env) throws IOException {

        super.init(source, options, env);
        sourceItr = source.deepCopy(env);
    }

    @Override
    public Value getTopValue() {

        if(hasTop()) {

            Value event = IteratorUtils.retrieveFullEvent(getTopKey(), sourceItr);
            return event;
        }

        return new Value("".getBytes());
    }
}
