package org.calrissian.accumulorecipes.lastn.support;


import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class LastNIterator implements Iterator<StoreEntry> {

    protected Scanner scanner;
    protected Iterator<Map.Entry<Key,Value>>  iterator;

    public LastNIterator(Scanner scanner) {
        this.scanner = scanner;
        this.iterator = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public StoreEntry next() {
        try {
            return ObjectMapperContext.getInstance().getObjectMapper()
                    .readValue(iterator.next().getValue().get(), StoreEntry.class);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return null;
    }

    @Override
    public void remove() {

        iterator.remove();
    }
}
