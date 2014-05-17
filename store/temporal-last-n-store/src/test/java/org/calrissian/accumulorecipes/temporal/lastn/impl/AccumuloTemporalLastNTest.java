package org.calrissian.accumulorecipes.temporal.lastn.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.domain.BaseEvent;
import org.calrissian.mango.domain.Event;
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

    Event testEntry = new BaseEvent(UUID.randomUUID().toString());
    testEntry.put(new Tuple("key1", "val1", ""));
    testEntry.put(new Tuple("key2", "val2", ""));

    Event testEntry2 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
    testEntry2.put(new Tuple("key1", "val1", ""));
    testEntry2.put(new Tuple("key2", "val2", ""));

    store.put("group", testEntry);
    store.put("group", testEntry2);

    Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            singleton("group"), 2, new Auths());

    Event actualEntry = Iterables.get(results, 0);
    assertEquals(actualEntry.getId(), testEntry.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry.getTuples()));


    actualEntry = Iterables.get(results, 1);
    assertEquals(actualEntry.getId(), testEntry2.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry2.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry2.getTuples()));
  }

  @Test
  public void testMultipleEntries_differentGroups() throws TableNotFoundException {

    Event testEntry = new BaseEvent(UUID.randomUUID().toString());
    testEntry.put(new Tuple("key1", "val1", ""));
    testEntry.put(new Tuple("key2", "val2", ""));

    Event testEntry2 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
    testEntry2.put(new Tuple("key1", "val1", ""));
    testEntry2.put(new Tuple("key2", "val2", ""));

    store.put("group", testEntry);
    store.put("group1", testEntry2);

    Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            Arrays.asList(new String[]{"group", "group1"}), 2, new Auths());

    Event actualEntry = Iterables.get(results, 0);
    assertEquals(actualEntry.getId(), testEntry.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry.getTuples()));


    actualEntry = Iterables.get(results, 1);
    assertEquals(actualEntry.getId(), testEntry2.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry2.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry2.getTuples()));
  }

  @Test
  public void testMultipleEntries_differentGroupsSomeInSameGroup() throws TableNotFoundException {

    Event testEntry = new BaseEvent(UUID.randomUUID().toString());
    testEntry.put(new Tuple("key1", "val1", ""));
    testEntry.put(new Tuple("key2", "val2", ""));

    Event testEntry2 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() - 5000);
    testEntry2.put(new Tuple("key1", "val1", ""));
    testEntry2.put(new Tuple("key2", "val2", ""));

    Event testEntry3 = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis() - 500);
    testEntry3.put(new Tuple("key1", "val1", ""));
    testEntry3.put(new Tuple("key2", "val2", ""));


    store.put("group", testEntry);
    store.put("group1", testEntry2);
    store.put("group1", testEntry3);


    Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            Arrays.asList(new String[]{"group", "group1"}), 3, new Auths());


    Event actualEntry = Iterables.get(results, 0);
    assertEquals(actualEntry.getId(), testEntry.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry.getTuples()));


    actualEntry = Iterables.get(results, 1);
    assertEquals(actualEntry.getId(), testEntry3.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry3.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry3.getTuples()));

    actualEntry = Iterables.get(results, 2);
    assertEquals(actualEntry.getId(), testEntry2.getId());
    assertEquals(actualEntry.getTimestamp(), testEntry2.getTimestamp());
    assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry2.getTuples()));
  }


}
