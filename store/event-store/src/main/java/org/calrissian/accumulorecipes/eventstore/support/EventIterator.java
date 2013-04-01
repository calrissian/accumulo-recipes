package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.types.TypeContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class EventIterator extends IntersectingIterator {

    protected SortedKeyValueIterator<Key,Value> sourceItr;

    public void init(SortedKeyValueIterator<Key,Value> source, java.util.Map<String,String> options, IteratorEnvironment env) throws IOException {

        super.init(source, options, env);
        sourceItr = source.deepCopy(env);
    }

    @Override
    public void next() throws IOException {
        super.next();
    }

    @Override
    public Value getTopValue() {

        if(hasTop()) {

            Key key = getTopKey();

            String eventUUID = key.getColumnQualifier().toString();

            Key startRangeKey = new Key(key.getRow(), new Text(AccumuloEventStore.SHARD_PREFIX_F +
                                        AccumuloEventStore.DELIM +
                                        eventUUID));
            Key stopRangeKey = new Key(key.getRow(), new Text(AccumuloEventStore.SHARD_PREFIX_F +
                                       AccumuloEventStore.DELIM +
                                       eventUUID + AccumuloEventStore.DELIM_END));

            Range eventRange = new Range(startRangeKey, stopRangeKey);

            Event event = new Event(eventUUID, key.getTimestamp());

            try {
                sourceItr.seek(eventRange, new ArrayList<ByteSequence>(), false);

                Collection<Tuple> tuples = new ArrayList<Tuple>();
                while(sourceItr.hasTop()) {

                    Key nextKey = sourceItr.getTopKey();
                    sourceItr.next();

                    if(!nextKey.getColumnFamily().toString().endsWith(eventUUID)) {
                        break;
                    }

                    String[] keyValueDatatype = nextKey.getColumnQualifier().toString().split(AccumuloEventStore.DELIM);

                    if(keyValueDatatype.length == 3) {

                        String tupleKey = keyValueDatatype[0];
                        String tupleType = keyValueDatatype[1];
                        Object tupleVal = TypeContext.getInstance().denormalize(keyValueDatatype[2], tupleType);

                        Tuple tuple = new Tuple(tupleKey, tupleVal, nextKey.getColumnVisibility().toString());
                        tuples.add(tuple);
                    }
                }

                if(tuples.size() > 0) {
                    event.putAll(tuples);
                    return new Value(ObjectMapperContext.getInstance().getObjectMapper().writeValueAsBytes(event));
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new Value("".getBytes());
    }
}
