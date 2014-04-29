package org.calrissian.accumulorecipes.commons.collect;

import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.collect.PeekingCloseableIterator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.calrissian.mango.collect.CloseableIterators.peekingIterator;
import static org.calrissian.mango.collect.CloseableIterators.wrap;

public class StoreEntryMergeJoinIterable implements Iterable<StoreEntry> {

    private Iterable<Iterable<StoreEntry>> cursors;

    public StoreEntryMergeJoinIterable(Iterable<Iterable<StoreEntry>> cursors) {
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
}
