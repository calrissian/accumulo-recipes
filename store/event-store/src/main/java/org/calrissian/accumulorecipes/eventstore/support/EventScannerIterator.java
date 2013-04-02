package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.collect.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


public class EventScannerIterator implements CloseableIterator<Event> {

    protected BatchScanner scanner;
    protected Iterator<Map.Entry<Key,Value>> iterator;

    public EventScannerIterator(BatchScanner scanner) {
        this.scanner = scanner;
        this.iterator = scanner.iterator();
    }

    @Override
    public void closeQuietly() {

        scanner.close();
    }

    @Override
    public void close() throws IOException {

        scanner.close();
    }

    @Override
    public Iterator<Event> iterator() {
        return new EventScannerIterator(scanner);
    }

    @Override
    public boolean hasNext() {

        return iterator.hasNext();
    }

    @Override
    public Event next() {

        try {
            String nextEvent = new String(iterator.next().getValue().get());
            return ObjectMapperContext.getInstance().getObjectMapper()
                    .readValue(nextEvent, Event.class);
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        iterator().remove();
    }
}
