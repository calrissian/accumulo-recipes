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
package org.calrissian.accumulorecipes.lastn.impl;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.lastn.support.LastNIterator;
import org.calrissian.commons.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloLastNStoreTest {

    AccumuloLastNStore store;
    Connector connector;

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {

        MockInstance instance = new MockInstance();
        connector = instance.getConnector("username", "password".getBytes());
        store = new AccumuloLastNStore(connector, 3);
    }


    @Test
    public void test() throws TableNotFoundException {

        StoreEntry entry1 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
        entry1.put(new Tuple("key1", "val1", ""));
        entry1.put(new Tuple("key3", "val3", ""));

        StoreEntry entry2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000);
        entry2.put(new Tuple("key1", "val1", ""));
        entry2.put(new Tuple("key3", "val3", ""));

        StoreEntry entry3 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000);
        entry3.put(new Tuple("key1", "val1", ""));
        entry3.put(new Tuple("key3", "val3", ""));

        StoreEntry entry4 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() + 5000);
        entry4.put(new Tuple("key1", "val1", ""));
        entry4.put(new Tuple("key3", "val3", ""));

        store.put("index1", entry1);
        store.put("index1", entry2);
        store.put("index1", entry3);
        store.put("index1", entry4);

        LastNIterator itr = (LastNIterator) store.get("index1", new Authorizations());

        int count = 0;
        while(itr.hasNext()) {
            if(count == 0) {
                assertEquals(entry4, itr.next());
            }

            else if(count == 1) {
                assertEquals(entry3,  itr.next());
            }

            else if(count == 2) {
                assertEquals(entry2,  itr.next());
            }

            else {
                fail();
            }

            count++;
        }

        assertEquals(3, count);
    }

    protected void printTable() throws TableNotFoundException {

        Scanner scanner = connector.createScanner(store.getTableName(), new Authorizations());
        for(Map.Entry<Key,Value> entry : scanner) {
            System.out.println(entry + "- " + new String(entry.getValue().get()));
        }
    }

}
