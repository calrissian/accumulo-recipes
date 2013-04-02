package org.calrissian.accumulorecipes.eventstore.impl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.criteria.builder.QueryBuilder;
import org.calrissian.criteria.domain.Node;
import org.junit.Test;
import org.junit.Before;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AccumuloEventStoreTest {

    AccumuloEventStore store;

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {

        MockInstance instance =  new MockInstance();
        Connector conn  = instance.getConnector("username", "password".getBytes());

        store = new AccumuloEventStore(conn);
    }

    @Test
    public void testQuery_AndQuery() throws Exception, AccumuloException, AccumuloSecurityException {

        Event event = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        Event event2 = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").endStatement().build();

        Iterator<Event> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations());

        Event actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(actualEvent, event);
        }

        else {
            assertEquals(actualEvent, event2);
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(actualEvent, event);
        }

        else {
            assertEquals(actualEvent, event2);
        }
    }

    @Test
    public void testQuery_OrQuery() throws Exception, AccumuloException, AccumuloSecurityException {

        Event event = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        Event event2 = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

        Node query = new QueryBuilder().or().eq("key3", "val3").eq("key2", "val2").endStatement().build();

        Iterator<Event> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations());

        Event actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(actualEvent, event);
        }

        else {
            assertEquals(actualEvent, event2);
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(actualEvent, event);
        }

        else {
            assertEquals(actualEvent, event2);
        }
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {

        Event event = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        Event event2 = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

        Node query = new QueryBuilder().eq("key1", "val1").build();

        Iterator<Event> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations());

        Event actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(actualEvent, event);
        }

        else {
            assertEquals(actualEvent, event2);
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(actualEvent, event);
        }

        else {
            assertEquals(actualEvent, event2);
        }
    }
}
