package org.calrissian.accumlorecipes.changelog.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.collect.CloseableIterator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class EntryIterator implements CloseableIterator<StoreEntry> {

    ObjectMapper objectMapper = ObjectMapperContext.getInstance().getObjectMapper();

    BatchScanner scanner;
    Iterator<Map.Entry<Key,Value>> itr;

    public EntryIterator(BatchScanner scanner) {

        this.scanner = scanner;
        this.itr = scanner.iterator();
    }
    @Override
    public void closeQuietly() {

        try {
            close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void close() throws IOException {

        scanner.close();

    }

    @Override
    public Iterator<StoreEntry> iterator() {
        return new EntryIterator(scanner);
    }

    @Override
    public boolean hasNext() {
        return itr.hasNext();
    }

    @Override
    public StoreEntry next() {

        if(hasNext()) {
            try {
                return objectMapper.readValue(new String(itr.next().getValue().get()), StoreEntry.class);
            } catch (IOException e) {
                throw new IterationInterruptedException("There was a problem deserializing from the row");
            }
        }

        else {
            throw new IterationInterruptedException("No more entries.");

        }
    }

    @Override
    public void remove() {

        itr.remove();
    }
}
