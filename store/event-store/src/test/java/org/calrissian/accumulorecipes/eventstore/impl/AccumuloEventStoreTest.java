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
package org.calrissian.accumulorecipes.eventstore.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.accumulorecipes.eventstore.model.EventIndex;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.*;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AccumuloEventStoreTest {


    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Test
    public void testGet() throws Exception {

        Connector connector = getConnector();
        AccumuloEventStore store = new AccumuloEventStore(connector);

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.save(asList(event, event2));

        Scanner scanner = connector.createScanner("eventStore_index", new Authorizations());
        for(Map.Entry<Key,Value> entry : scanner) {
          System.out.println(entry);
        }

        CloseableIterable<StoreEntry> actualEvent = store.get(Collections.singletonList(new EventIndex(event.getId())), new Auths());

        assertEquals(1, Iterables.size(actualEvent));
        assertEquals(actualEvent.iterator().next(), event);
    }

    @Test
    public void testQuery_AndQuery() throws Exception {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.save(asList(event, event2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").endStatement().build();

        Iterator<StoreEntry> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, new Auths()).iterator();

        StoreEntry actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event.getTuples()));
        }

        else {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event2.getTuples()));
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event.getTuples()));
        }

        else {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event2.getTuples()));
        }
    }

    @Test
    public void testQuery_OrQuery() throws Exception {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.save(asList(event, event2));

        Node query = new QueryBuilder().or().eq("key3", "val3").eq("key2", "val2").endStatement().build();

        Iterator<StoreEntry> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, new Auths()).iterator();

        StoreEntry actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        }
        else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        }
        else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.save(asList(event, event2));

        Node query = new QueryBuilder().eq("key1", "val1").build();

        Iterator<StoreEntry> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, new Auths()).iterator();

        StoreEntry actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        }

        else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        }

        else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }
    }

    @Test
    public void testQuery_MultipleNotInQuery() throws Exception {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(),
                currentTimeMillis());
        event.put(new Tuple("hasIp", "true", ""));
        event.put(new Tuple("ip", "1.1.1.1", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(),
                currentTimeMillis());
        event2.put(new Tuple("hasIp", "true", ""));
        event2.put(new Tuple("ip", "2.2.2.2", ""));

        StoreEntry event3 = new StoreEntry(UUID.randomUUID().toString(),
                currentTimeMillis());
        event3.put(new Tuple("hasIp", "true", ""));
        event3.put(new Tuple("ip", "3.3.3.3", ""));

        store.save(asList(event, event2, event3));

        Node query = new QueryBuilder()
                .and()
                .notEq("ip", "1.1.1.1")
                .notEq("ip", "2.2.2.2")
                .notEq("ip", "4.4.4.4")
                .eq("hasIp", "true")
                .endStatement().build();

        Iterator<StoreEntry> itr = store.query(
                new Date(currentTimeMillis() - 5000), new Date(), query,
                new Auths()).iterator();

        int x = 0;

        while (itr.hasNext()) {
            x++;
            StoreEntry e = itr.next();
            assertEquals("3.3.3.3",e.get("ip").getValue());

        }
        assertEquals(1, x);
    }

}
