package org.calrissian.accumulorecipes.temporal.lastn.support;

import com.google.common.base.Function;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.temporal.lastn.impl.AccumuloTemporalLastNStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.PeekingCloseableIterator;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.types.TypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.temporal.lastn.impl.AccumuloTemporalLastNStore.DELIM;
import static org.calrissian.accumulorecipes.temporal.lastn.iterators.EventGroupingIterator.decodeRow;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;
import static org.calrissian.mango.collect.CloseableIterators.peekingIterator;
import static org.calrissian.mango.collect.CloseableIterators.wrap;

public class MergeJoinIterable implements Iterable<StoreEntry> {

    private TypeRegistry registry = ACCUMULO_TYPES;

    private List<Iterable<Map.Entry<Key,Value>>> cursors;

    public MergeJoinIterable(List<Iterable<Map.Entry<Key,Value>>> cursors) {
        this.cursors = cursors;
    }

    @Override
    public Iterator iterator() {

        final List<PeekingCloseableIterator<Map.Entry<Key,Value>>> iterators =
                new ArrayList<PeekingCloseableIterator<Map.Entry<Key,org.apache.accumulo.core.data.Value>>>();

        for(Iterable<Map.Entry<Key,Value>> entries : cursors)
            iterators.add(peekingIterator(wrap(entries.iterator())));

        return new Iterator<StoreEntry>() {
            @Override
            public boolean hasNext() {

                for(Iterator<Map.Entry<Key,Value>> entry : iterators) {
                    if(entry.hasNext())
                        return true;
                }
                return false;
            }

            @Override
            public StoreEntry next() {

                PeekingCloseableIterator<Map.Entry<Key, Value>> curEntry = null;
                for (PeekingCloseableIterator<Map.Entry<Key, Value>> itr : iterators) {
                    if (itr.hasNext() && (curEntry == null ||
                            (itr.peek()).getKey().getTimestamp() > curEntry.peek().getKey().getTimestamp()))
                        curEntry = itr;
                }

                Map.Entry<Key,Value> entry = curEntry.next();
                try {
                    StoreEntry toReturn = entryXform.apply(decodeRow(entry.getKey(), entry.getValue()));
                    return toReturn;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                for(Iterator<Map.Entry<Key, Value>> itr : iterators)
                    itr.remove();
            }
        };

    }

    Function<List<Map.Entry<Key,Value>>, StoreEntry> entryXform = new Function<List<Map.Entry<Key, Value>>, StoreEntry>() {
        @Override
        public StoreEntry apply(List<Map.Entry<Key, Value>> entries) {
            StoreEntry toReturn = null;
            try {
                for(Map.Entry<Key,Value> tupleCol : entries) {
                    String[] splits = splitPreserveAllTokens(new String(tupleCol.getValue().get()), DELIM);
                    if(toReturn == null) {
                        toReturn = new StoreEntry(splits[0], Long.parseLong(splits[1]));
                    }
                    toReturn.put(new Tuple(splits[2], registry.decode(splits[3], splits[4]), splits[5]));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return toReturn;
       }
    };

}
