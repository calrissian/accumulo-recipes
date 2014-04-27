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
import java.util.*;

import static org.apache.commons.lang.StringUtils.splitPreserveAllTokens;
import static org.calrissian.accumulorecipes.temporal.lastn.impl.AccumuloTemporalLastNStore.DELIM;
import static org.calrissian.accumulorecipes.temporal.lastn.iterators.EventGroupingIterator.decodeRow;
import static org.calrissian.mango.accumulo.types.AccumuloTypeEncoders.ACCUMULO_TYPES;
import static org.calrissian.mango.collect.CloseableIterators.peekingIterator;
import static org.calrissian.mango.collect.CloseableIterators.wrap;

public class MergeJoinIterable implements Iterable<StoreEntry> {

    private TypeRegistry registry = ACCUMULO_TYPES;

    private Iterable<Iterable<StoreEntry>> cursors;

    public MergeJoinIterable(Iterable<Iterable<StoreEntry>> cursors) {
        this.cursors = cursors;
    }

    @Override
    public Iterator iterator() {

        final List<PeekingCloseableIterator<StoreEntry>> iterators =
                new LinkedList<PeekingCloseableIterator<StoreEntry>>();
        for(Iterable<StoreEntry> entries : cursors)
            iterators.add(peekingIterator(wrap(entries.iterator())));

        return new Iterator<StoreEntry>() {
            @Override
            public boolean hasNext() {

                for(Iterator<StoreEntry> entry : iterators) {
                    if(entry.hasNext())
                        return true;
                }
                return false;
            }

            @Override
            public StoreEntry next() {

                PeekingCloseableIterator<StoreEntry> curEntry = null;
                for (PeekingCloseableIterator<StoreEntry> itr : iterators) {
                    if (itr.hasNext() && (curEntry == null ||
                            (itr.peek()).getTimestamp() > curEntry.peek().getTimestamp()))
                        curEntry = itr;
                }

                return curEntry.next();
            }

            @Override
            public void remove() {
                for(Iterator<StoreEntry> itr : iterators)
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
