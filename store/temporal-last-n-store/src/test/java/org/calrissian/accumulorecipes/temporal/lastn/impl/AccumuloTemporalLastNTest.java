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

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import java.util.Date;
import java.util.HashSet;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;

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

        Event testEntry = EventBuilder.create("", randomUUID().toString(), currentTimeMillis())
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        Event testEntry2 = EventBuilder.create("", randomUUID().toString(), currentTimeMillis() - 5000)
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        store.put("group", testEntry);
        store.put("group", testEntry2);

        AccumuloTestUtils.dumpTable(connector, "temporalLastN");

        Iterable<Event> results = store.get(new Date(currentTimeMillis() - 500000), new Date(currentTimeMillis() + 500000),
            singleton("group"), 2, Auths.EMPTY);

        Event actualEntry = Iterables.get(results, 0);
        assertEquals(actualEntry.getId(), testEntry.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry.getAttributes()));


        actualEntry = Iterables.get(results, 1);
        assertEquals(actualEntry.getId(), testEntry2.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry2.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry2.getAttributes()));
    }

    @Test
    public void testTimeLimit_downToMillis() throws TableNotFoundException {

        long curTime = currentTimeMillis();
        Event testEntry = EventBuilder.create("", randomUUID().toString(), curTime)
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        Event testEntry2 = EventBuilder.create("", randomUUID().toString(), curTime - 5000)
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        store.put("group", testEntry);
        store.put("group", testEntry2);

        Iterable<Event> results = store.get(new Date(curTime - 4999), new Date(curTime + 50000),
            singleton("group"), 2, Auths.EMPTY);

        Event actualEntry = Iterables.get(results, 0);
        assertEquals(actualEntry.getId(), testEntry.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry.getAttributes()));

        assertEquals(1, Iterables.size(results));


        results = store.get(new Date(curTime - 5001), new Date(curTime + 50000),
            singleton("group"), 2, Auths.EMPTY);

        assertEquals(2, Iterables.size(results));
    }

    @Test
    public void testMultipleEntries_differentGroups() throws TableNotFoundException {

        Event testEntry = EventBuilder.create("", randomUUID().toString(), currentTimeMillis())
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        Event testEntry2 = EventBuilder.create("", randomUUID().toString(), currentTimeMillis() - 5000)
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        store.put("group", testEntry);
        store.put("group1", testEntry2);

        AccumuloTestUtils.dumpTable(connector, "temporalLastN");



        Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            Sets.newHashSet("group", "group1"), 2, Auths.EMPTY);

        Event actualEntry = Iterables.get(results, 0);
        assertEquals(actualEntry.getId(), testEntry.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry.getAttributes()));


        actualEntry = Iterables.get(results, 1);
        assertEquals(actualEntry.getId(), testEntry2.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry2.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry2.getAttributes()));
    }

    @Test
    public void testMultipleEntries_differentGroupsSomeInSameGroup() throws Exception {

        Event testEntry = EventBuilder.create("", randomUUID().toString(), currentTimeMillis())
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        Event testEntry2 = EventBuilder.create("", randomUUID().toString(), currentTimeMillis() - 5000)
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();

        Event testEntry3 = EventBuilder.create("", randomUUID().toString(), currentTimeMillis() - 500)
                .attr("key1", "val1")
                .attr("key2", "val2")
                .build();


        store.put("group", testEntry);
        store.put("group1", testEntry2);
        store.put("group1", testEntry3);

        store.flush();

        AccumuloTestUtils.dumpTable(connector, "temporalLastN");


        Iterable<Event> results = store.get(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis() + 50000),
            Sets.newHashSet("group", "group1"), 3, Auths.EMPTY);


        Event actualEntry = Iterables.get(results, 0);
        assertEquals(actualEntry.getId(), testEntry.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry.getAttributes()));


        actualEntry = Iterables.get(results, 1);
        assertEquals(actualEntry.getId(), testEntry3.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry3.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry3.getAttributes()));

        actualEntry = Iterables.get(results, 2);
        assertEquals(actualEntry.getId(), testEntry2.getId());
        assertEquals(actualEntry.getTimestamp(), testEntry2.getTimestamp());
        assertEquals(new HashSet(actualEntry.getAttributes()), new HashSet(testEntry2.getAttributes()));
    }


}
