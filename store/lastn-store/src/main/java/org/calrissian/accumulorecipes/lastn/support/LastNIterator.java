package org.calrissian.accumulorecipes.lastn.support;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * A simple java iterator for returning StoreEntry objects from a scanner that's been configured with the
 * Accumulo EntryIterator.
 */
public class LastNIterator implements Iterator<StoreEntry> {

    protected Iterator<Map.Entry<Key,Value>>  iterator;

    public LastNIterator(Scanner scanner) {
        this.iterator = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    /**
     * Deserializes the next StoreEntry object from the EntryIterator and returns it
     * @return
     */
    @Override
    public StoreEntry next() {
        try {
            return ObjectMapperContext.getInstance().getObjectMapper()
                    .readValue(iterator.next().getValue().get(), StoreEntry.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {

        iterator.remove();
    }
}
