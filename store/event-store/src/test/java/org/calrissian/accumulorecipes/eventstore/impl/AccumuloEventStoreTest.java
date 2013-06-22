package org.calrissian.accumulorecipes.eventstore.impl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Tuple;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AccumuloEventStoreTest {


    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Test
    public void testGet() throws Exception {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.save(Collections.singleton(event));
        store.save(Collections.singleton(event2));

        StoreEntry actualEvent = store.get(event.getId(), new Authorizations());

        assertEquals(actualEvent, event);
    }

    @Test
    public void testQuery_AndQuery() throws Exception, AccumuloException, AccumuloSecurityException {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.save(Collections.singleton(event));
        store.save(Collections.singleton(event2));

        Node query = new QueryBuilder().and().eq("key1", "val1").eq("key2", "val2").endStatement().build();

        Iterator<StoreEntry> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations()).iterator();

        StoreEntry actualEvent = itr.next();
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
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.save(Collections.singleton(event));
        store.save(Collections.singleton(event2));

        Node query = new QueryBuilder().or().eq("key3", "val3").eq("key2", "val2").endStatement().build();

        Iterator<StoreEntry> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations()).iterator();

        StoreEntry actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(event, actualEvent);
        }

        else {
            assertEquals(event2, actualEvent);
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(event, actualEvent);
        }

        else {
            assertEquals(event2, actualEvent);
        }
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {
        AccumuloEventStore store = new AccumuloEventStore(getConnector());

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.save(Collections.singleton(event));
        store.save(Collections.singleton(event2));

        Node query = new QueryBuilder().eq("key1", "val1").build();

        Iterator<StoreEntry> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations()).iterator();

        StoreEntry actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(event, actualEvent);
        }

        else {
            assertEquals(event2, actualEvent);
        }

        actualEvent = itr.next();
        if(actualEvent.getId().equals(event.getId())) {
            assertEquals(event, actualEvent);
        }

        else {
            assertEquals(event2, actualEvent);
        }
    }

}
