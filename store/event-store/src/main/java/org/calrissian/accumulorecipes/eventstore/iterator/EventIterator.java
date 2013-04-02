package org.calrissian.accumulorecipes.eventstore.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import java.io.IOException;

import static org.calrissian.accumulorecipes.eventstore.support.Constants.DELIM;
import static org.calrissian.accumulorecipes.eventstore.support.Constants.SHARD_PREFIX_B;

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

            Key topKey = getTopKey();

            String colFam = topKey.getColumnFamily().toString();
            String eventuUUID = null;

            if(colFam.startsWith(SHARD_PREFIX_B)) {
                eventuUUID = topKey.getColumnQualifier().toString();
            }

            else {
                eventuUUID = colFam.split(DELIM)[1];

            }

            Value event = IteratorUtils.retrieveFullEvent(eventuUUID, topKey, sourceItr);
            return event;
        }

        return new Value("".getBytes());
    }
}
