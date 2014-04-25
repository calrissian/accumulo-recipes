package org.calrissian.accumulorecipes.temporal.lastn.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.domain.StoreEntry;
import org.calrissian.mango.domain.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class AccumuloTemporalLastNTest {

    Connector connector;
    AccumuloTemporalLastNStore store;

    @Before
    public void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Instance instance = new MockInstance();
        connector = instance.getConnector("root", "".getBytes());
        store = new AccumuloTemporalLastNStore(connector);
    }

    @Test
    public void testMultipleEntries_sameGroup() throws TableNotFoundException {

        StoreEntry testEntry = new StoreEntry(UUID.randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1", ""));
        testEntry.put(new Tuple("key2", "val2", ""));

        StoreEntry testEntry2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
        testEntry2.put(new Tuple("key1", "val1", ""));
        testEntry2.put(new Tuple("key2", "val2", ""));

        store.put("group", testEntry);
        store.put("group", testEntry2);

        Iterable<StoreEntry> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
                singleton("group"), 2, new Auths());

        assertEquals(testEntry, Iterables.get(results, 0));
        assertEquals(testEntry2, Iterables.get(results,1));
    }

    @Test
    public void testMultipleEntries_differentGroups() throws TableNotFoundException {

        StoreEntry testEntry = new StoreEntry(UUID.randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1", ""));
        testEntry.put(new Tuple("key2", "val2", ""));

        StoreEntry testEntry2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
        testEntry2.put(new Tuple("key1", "val1", ""));
        testEntry2.put(new Tuple("key2", "val2", ""));

        store.put("group", testEntry);
        store.put("group1", testEntry2);

        Iterable<StoreEntry> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
                Arrays.asList(new String[] {"group", "group1"}), 2, new Auths());

        assertEquals(testEntry, Iterables.get(results,0));
        assertEquals(testEntry2, Iterables.get(results,1));
    }

    @Test
    public void testMultipleEntries_differentGroupsSomeInSameGroup() throws TableNotFoundException {

        StoreEntry testEntry = new StoreEntry(UUID.randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1", ""));
        testEntry.put(new Tuple("key2", "val2", ""));

        StoreEntry testEntry2 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
        testEntry2.put(new Tuple("key1", "val1", ""));
        testEntry2.put(new Tuple("key2", "val2", ""));

        StoreEntry testEntry3 = new StoreEntry(UUID.randomUUID().toString(), System.currentTimeMillis() - 500);
        testEntry3.put(new Tuple("key1", "val1", ""));
        testEntry3.put(new Tuple("key2", "val2", ""));


        store.put("group", testEntry);
        store.put("group1", testEntry2);
        store.put("group1", testEntry3);


        Iterable<StoreEntry> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
                Arrays.asList(new String[] {"group", "group1"}), 3, new Auths());

        assertEquals(testEntry, Iterables.get(results,0));
        assertEquals(testEntry3, Iterables.get(results,1));
        assertEquals(testEntry2, Iterables.get(results,2));
    }




}
