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
package org.calrissian.accumulorecipes.temporal.lastn.impl;

import java.util.Date;
import java.util.HashSet;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Before;
import org.junit.Test;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
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

        Event testEntry = new BaseEvent(randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1"));
        testEntry.put(new Tuple("key2", "val2"));

        Event testEntry2 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 5000);
        testEntry2.put(new Tuple("key1", "val1"));
        testEntry2.put(new Tuple("key2", "val2"));

        store.put("group", testEntry);
        store.put("group", testEntry2);

        AccumuloTestUtils.dumpTable(connector, "temporalLastN");

        Iterable<Event> results = store.get(new Date(currentTimeMillis() - 500000), new Date(currentTimeMillis() + 500000),
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
    public void testTimeLimit_downToMillis() throws TableNotFoundException {

        long curTime = currentTimeMillis();
        Event testEntry = new BaseEvent(randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1"));
        testEntry.put(new Tuple("key2", "val2"));

        Event testEntry2 = new BaseEvent(randomUUID().toString(), curTime - 5000);
        testEntry2.put(new Tuple("key1", "val1"));
        testEntry2.put(new Tuple("key2", "val2"));

        store.put("group", testEntry);
        store.put("group", testEntry2);

        Iterable<Event> results = store.get(new Date(curTime - 4999), new Date(curTime + 50000),
            singleton("group"), 2, new Auths());

        Event actualEntry = Iterables.get(results, 0);
        assertEquals(actualEntry.getId(), testEntry.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
        assertEquals(new HashSet(actualEntry.getTuples()), new HashSet(testEntry.getTuples()));

        assertEquals(1, Iterables.size(results));


        results = store.get(new Date(curTime - 5001), new Date(curTime + 50000),
            singleton("group"), 2, new Auths());

        assertEquals(2, Iterables.size(results));
    }

    @Test
    public void testMultipleEntries_differentGroups() throws TableNotFoundException {

        Event testEntry = new BaseEvent(randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1"));
        testEntry.put(new Tuple("key2", "val2"));

        Event testEntry2 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 5000);
        testEntry2.put(new Tuple("key1", "val1"));
        testEntry2.put(new Tuple("key2", "val2"));

        store.put("group", testEntry);
        store.put("group1", testEntry2);

        AccumuloTestUtils.dumpTable(connector, "temporalLastN");



        Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            asList(new String[] {"group", "group1"}), 2, new Auths());

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
    public void testMultipleEntries_differentGroupsSomeInSameGroup() throws Exception {

        Event testEntry = new BaseEvent(randomUUID().toString());
        testEntry.put(new Tuple("key1", "val1"));
        testEntry.put(new Tuple("key2", "val2"));

        Event testEntry2 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 5000);
        testEntry2.put(new Tuple("key1", "val1"));
        testEntry2.put(new Tuple("key2", "val2"));

        Event testEntry3 = new BaseEvent(randomUUID().toString(), currentTimeMillis() - 500);
        testEntry3.put(new Tuple("key1", "val1"));
        testEntry3.put(new Tuple("key2", "val2"));


        store.put("group", testEntry);
        store.put("group1", testEntry2);
        store.put("group1", testEntry3);

        store.flush();

        AccumuloTestUtils.dumpTable(connector, "temporalLastN");


        Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            asList(new String[] {"group", "group1"}), 3, new Auths());


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
