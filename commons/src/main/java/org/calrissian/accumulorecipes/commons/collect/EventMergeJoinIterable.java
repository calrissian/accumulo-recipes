package org.calrissian.accumulorecipes.commons.collect;

import org.calrissian.mango.collect.PeekingCloseableIterator;
import org.calrissian.mango.domain.Event;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.calrissian.mango.collect.CloseableIterators.peekingIterator;
import static org.calrissian.mango.collect.CloseableIterators.wrap;

public class EventMergeJoinIterable implements Iterable<Event> {

    private Iterable<Iterable<Event>> cursors;

    public EventMergeJoinIterable(Iterable<Iterable<Event>> cursors) {
        this.cursors = cursors;
    }

    @Override
    public Iterator<Event> iterator() {

        final List<PeekingCloseableIterator<Event>> iterators =
                new LinkedList<PeekingCloseableIterator<Event>>();
        for(Iterable<Event> entries : cursors)
            iterators.add(peekingIterator(wrap(entries.iterator())));

        return new Iterator<Event>() {
            @Override
            public boolean hasNext() {

                for(Iterator<Event> entry : iterators) {
                    if(entry.hasNext())
                        return true;
                }
                return false;
            }

            @Override
            public Event next() {

                PeekingCloseableIterator<Event> curEntry = null;
                for (PeekingCloseableIterator<Event> itr : iterators) {
                    if (itr.hasNext() && (curEntry == null ||
                            (itr.peek()).getTimestamp() > curEntry.peek().getTimestamp()))
                        curEntry = itr;
                }

                return curEntry.next();
            }

            @Override
            public void remove() {
                for(Iterator<Event> itr : iterators)
                    itr.remove();
            }
        };

    }
}
