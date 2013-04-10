/*
 * Copyright (C) 2013 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.eventstore.support;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.collect.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


public class EventScannerIterator implements CloseableIterator<StoreEntry> {

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
    public Iterator<StoreEntry> iterator() {
        return new EventScannerIterator(scanner);
    }

    @Override
    public boolean hasNext() {

        return iterator.hasNext();
    }

    @Override
    public StoreEntry next() {

        try {
            String nextEvent = new String(iterator.next().getValue().get());
            return ObjectMapperContext.getInstance().getObjectMapper()
                    .readValue(nextEvent, StoreEntry.class);
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        iterator().remove();
    }
}
