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
