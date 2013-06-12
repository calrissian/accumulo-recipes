package org.calrissian.accumulorecipes.eventstore.impl;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.criteria.builder.QueryBuilder;
import org.calrissian.criteria.domain.Node;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class AccumuloEventStoreTest {

    AccumuloEventStore store;
    Connector conn;

    @Before
    public void setUp() throws AccumuloException, AccumuloSecurityException {

        MockInstance instance =  new MockInstance();
        conn  = instance.getConnector("username", "password".getBytes());

        store = new AccumuloEventStore(conn);
    }

    @Test
    public void testGet() throws Exception {

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

        StoreEntry actualEvent = store.get(event.getId(), new Authorizations());

        assertEquals(actualEvent, event);
    }

    @Test
    public void testQuery_AndQuery() throws Exception, AccumuloException, AccumuloSecurityException {

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key2", "val2", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

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

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

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

        StoreEntry event = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        StoreEntry event2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis());
        event2.put(new Tuple("key1", "val1", ""));
        event2.put(new Tuple("key3", "val3", ""));

        store.put(Collections.singleton(event));
        store.put(Collections.singleton(event2));

        Node query = new QueryBuilder().eq("key1", "val1").build();

        Iterator<StoreEntry> itr = store.query(new Date(System.currentTimeMillis() - 5000),
                new Date(System.currentTimeMillis()), query, new Authorizations()).iterator();


        printTable();

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

    protected void printTable() throws TableNotFoundException {

        Scanner scanner = conn.createScanner(store.getShardTable(), new Authorizations());
        for(Map.Entry<Key,Value> entry : scanner) {
            System.out.println(entry + "- " + new String(entry.getValue().get()));
        }
    }

}
