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

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventIndex;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.google.common.collect.Iterables.size;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AccumuloEventStoreTest {


    private Connector connector;
    private EventStore store;

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        connector = getConnector();
        store = new AccumuloEventStore(connector);
    }

    @Test
    public void testGet() throws Exception {

        long time = currentTimeMillis();
        Event event = new BaseEvent(UUID.randomUUID().toString(), time);
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), time);
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));

        store.save(asList(event, event2));

        Scanner scanner = connector.createScanner("eventStore_index", new Authorizations());
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
        }

        CloseableIterable<Event> actualEvent = store.get(singletonList(new EventIndex(event.getId(), event.getTimestamp())), null, new Auths());

        assertEquals(1, size(actualEvent));
        Event actual = actualEvent.iterator().next();
        assertEquals(new HashSet(actual.getTuples()), new HashSet(event.getTuples()));
        assertEquals(actual.getId(), event.getId());

        actualEvent = store.get(singletonList(new EventIndex(event.getId(), time)), null, new Auths());

        assertEquals(1, size(actualEvent));
        actual = actualEvent.iterator().next();
        assertEquals(new HashSet(actual.getTuples()), new HashSet(event.getTuples()));
        assertEquals(actual.getId(), event.getId());

    }

    @Test
    public void testQueryKeyNotInIndex() {

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));

        store.save(asList(event, event2));

        Node query = new QueryBuilder().and().eq("key4", "val5").end().build();

        Iterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, new Auths());

        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQueryRangeNotInIndex() {

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));

        store.save(asList(event, event2));

        Node query = new QueryBuilder().and().range("key4", 0, 5).end().build();

        Iterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, new Auths());

        assertEquals(0, Iterables.size(itr));
    }



    @Test
    public void testGreaterThan() throws Exception {

        long time = currentTimeMillis();
        Event event = new BaseEvent(UUID.randomUUID().toString(), time);
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", 1));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), time);
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", 10));

        store.save(asList(event, event2));
        store.flush();

        Scanner scanner = connector.createScanner("eventStore_index", new Authorizations());
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
        }

        CloseableIterable<Event> actualEvent = store.query(new Date(time-50), new Date(time+50), new QueryBuilder().greaterThan("key2", 9).build(), null, new Auths());

        assertEquals(1, size(actualEvent));
        Event actual = actualEvent.iterator().next();
        assertEquals(new HashSet(event2.getTuples()), new HashSet(actual.getTuples()));
        assertEquals(actual.getId(), event2.getId());

        actualEvent = store.query(new Date(time), new Date(time), new QueryBuilder().greaterThan("key2", 0).build(), null, new Auths());


        assertEquals(2, size(actualEvent));

    }

    @Test
    public void test_TimeLimit() throws Exception {

        long currentTime = System.currentTimeMillis();

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTime);
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTime - 5000);
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));

        store.save(asList(event, event2));

        Node node = new QueryBuilder().eq("key1", "val1").build();

        CloseableIterable<Event> actualEvent = store.query(new Date(currentTime - 5001), new Date(currentTime + 500), node, null, new Auths());
        assertEquals(2, size(actualEvent));

        actualEvent = store.query(new Date(currentTime - 3000), new Date(currentTime+50), node, null, new Auths());
        assertEquals(1, size(actualEvent));
    }

    @Test
    public void testGet_withSelection() throws Exception {

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));

        store.save(asList(event, event2));

        CloseableIterable<Event> actualEvent = store.get(singletonList(new EventIndex(event.getId(), event.getTimestamp())),
                Collections.singleton("key1"), new Auths());

        assertEquals(1, size(actualEvent));
        assertNull(actualEvent.iterator().next().get("key2"));
        assertNotNull(actualEvent.iterator().next().get("key1"));
    }


    @Test
    public void testQuery_withSelection() throws Exception {

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));


        store.save(asList(event, event2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, Collections.singleton("key1"), new Auths());

        int count = 0;
        for (Event entry : itr) {
            count++;
            assertNull(entry.get("key2"));
            assertNotNull(entry.get("key1"));
        }
        assertEquals(2, count);
    }

    @Test
    public void testQuery_AndQuery() throws Exception {

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));

        store.save(asList(event, event2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterator<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, new Auths()).iterator();

        Event actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event.getTuples()));
        } else {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event2.getTuples()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event.getTuples()));
        } else {
            assertEquals(new HashSet(actualEvent.getTuples()), new HashSet(event2.getTuples()));
        }
    }

    @Test
    public void testQuery_OrQuery() throws Exception {


        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key3", "val3"));

        store.save(asList(event, event2));

        AccumuloTestUtils.dumpTable(connector, "eventStore_index");
        AccumuloTestUtils.dumpTable(connector, "eventStore_shard");


        Node query = new QueryBuilder().or().eq("key3", "val3").eq("key2", "val2").end().build();

        Iterator<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, new Auths()).iterator();

        Event actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        } else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        } else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {

        Event event = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(), currentTimeMillis());
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key3", "val3"));

        store.save(asList(event, event2));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard");

        Node query = new QueryBuilder().eq("key1", "val1").build();

        Iterator<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, new Auths()).iterator();

        Event actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        } else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getTuples()), new HashSet(actualEvent.getTuples()));
        } else {
            assertEquals(new HashSet(event2.getTuples()), new HashSet(actualEvent.getTuples()));
        }
    }

    @Test
    public void testQuery_emptyNodeReturnsNoResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Node query = new QueryBuilder().and().end().build();

        CloseableIterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, new Auths());

        assertEquals(0, size(itr));
    }

    @Test
    public void testQuery_MultipleNotInQuery() throws Exception {

        Event event = new BaseEvent(UUID.randomUUID().toString(),
                currentTimeMillis());
        event.put(new Tuple("hasIp", "true"));
        event.put(new Tuple("ip", "1.1.1.1"));

        Event event2 = new BaseEvent(UUID.randomUUID().toString(),
                currentTimeMillis());
        event2.put(new Tuple("hasIp", "true"));
        event2.put(new Tuple("ip", "2.2.2.2"));

        Event event3 = new BaseEvent(UUID.randomUUID().toString(),
                currentTimeMillis());
        event3.put(new Tuple("hasIp", "true"));
        event3.put(new Tuple("ip", "3.3.3.3"));

        store.save(asList(event, event2, event3));

        Node query = new QueryBuilder()
                .and()
                .notEq("ip", "1.1.1.1")
                .notEq("ip", "2.2.2.2")
                .notEq("ip", "4.4.4.4")
                .eq("hasIp", "true")
                .end().build();

        Iterator<Event> itr = store.query(
                new Date(currentTimeMillis() - 5000), new Date(), query,
                null, new Auths()).iterator();

        int x = 0;

        while (itr.hasNext()) {
            x++;
            Event e = itr.next();
            assertEquals("3.3.3.3", e.get("ip").getValue());

        }
        assertEquals(1, x);
    }

    @Ignore
    @Test
    public void testQuery_has() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Event event = new BaseEvent("id");
        event.put(new Tuple("key1", "val1"));

        store.save(asList(event));

        Node node = new QueryBuilder().has("key1").build();

        Iterable<Event> itr = store.query(new Date(0), new Date(), node, null, new Auths());
        assertEquals(1, Iterables.size(itr));
        assertEquals(event, Iterables.get(itr, 0));
    }

    @Ignore
    @Test
    public void testQuery_hasNot() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Event event = new BaseEvent("id");
        event.put(new Tuple("key1", "val1"));

        store.save(asList(event));

        Node node = new QueryBuilder().hasNot("key2").build();

        Iterable<Event> itr = store.query(new Date(0), new Date(), node, null, new Auths());
        assertEquals(1, Iterables.size(itr));
        assertEquals(event, Iterables.get(itr, 0));
    }


}
