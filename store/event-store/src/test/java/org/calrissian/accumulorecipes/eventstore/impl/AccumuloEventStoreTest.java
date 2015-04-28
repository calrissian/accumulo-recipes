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
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.attribute.MetadataBuilder;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.support.shard.DailyShardBuilder;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.calrissian.mango.domain.event.EventIdentifier;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static com.google.common.collect.Iterables.size;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.*;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.*;

public class AccumuloEventStoreTest {


    private Connector connector;
    private EventStore store;

    private Map<String, String> meta = new MetadataBuilder().setVisibility("A").build();
    private Auths DEFAULT_AUTHS = new Auths("A");

    public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        return new MockInstance().getConnector("root", "".getBytes());
    }

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        connector = getConnector();
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("A"));
        store = new AccumuloEventStore(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_STORE_CONFIG, LEXI_TYPES, new DailyShardBuilder(1));
    }

    @Test
    public void testGet() throws Exception {

        long time = currentTimeMillis();
        Event event = EventBuilder.create("", UUID.randomUUID().toString(), time)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), time)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard", new Authorizations("A"));

        CloseableIterable<Event> actualEvent = store.get(singletonList(new EventIdentifier("", event.getId(), event.getTimestamp())), null, DEFAULT_AUTHS);

        assertEquals(1, size(actualEvent));
        Event actual = actualEvent.iterator().next();
        assertEquals(new HashSet(actual.getAttributes()), new HashSet(event.getAttributes()));
        assertEquals(actual.getId(), event.getId());
        System.out.println(actual);

        actualEvent = store.get(singletonList(new EventIdentifier("", event.getId(), time)), null, DEFAULT_AUTHS);

        assertEquals(1, size(actualEvent));
        actual = actualEvent.iterator().next();
        assertEquals(new HashSet(actual.getAttributes()), new HashSet(event.getAttributes()));
        assertEquals(actual.getId(), event.getId());

    }

    @Test
    public void testGet_withVisibilities() {

        Map<String, String> shouldntSee = new MetadataBuilder().setVisibility("A&B").build();

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", shouldntSee))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", shouldntSee))
                .build();

        store.save(asList(event, event2));

        List<EventIdentifier> indexes = asList(new EventIdentifier[] {
                new EventIdentifier(event.getId(), event.getTimestamp()),
                new EventIdentifier(event2.getId(), event2.getTimestamp())
        });

        Iterable<Event> actualEvent1 = store.get(indexes, null, new Auths("A"));

        assertEquals(2, Iterables.size(actualEvent1));
        assertEquals(1, Iterables.get(actualEvent1, 0).size());
        assertEquals(1, Iterables.get(actualEvent1, 1).size());
    }

    @Test
    public void testVisibilityForAndQuery_noResults() {

        Map<String, String> shouldntSee = new MetadataBuilder().setVisibility("A&B").build();

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", shouldntSee))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", shouldntSee))
                .build();

        store.save(asList(event, event2));

        Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterable<Event> actualEvent1 = store.query(new Date(event.getTimestamp() - 500),
                new Date(event.getTimestamp() + 500), query, null, new Auths("A"));

        assertEquals(0, Iterables.size(actualEvent1));

    }

    @Test
    public void testVisibilityForQuery_resultsReturned() {

        Map<String, String> canSee = new MetadataBuilder().setVisibility("A&B").build();
        Map<String, String> cantSee = new MetadataBuilder().setVisibility("A&B&C").build();

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", canSee))
                .attr(new Attribute("key3", "val3", cantSee))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", canSee))
                .attr(new Attribute("key3", "val3", cantSee))
                .build();

        store.save(asList(event, event2));

        Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterable<Event> actualEvent1 = store.query(new Date(event.getTimestamp() - 500),
                new Date(event.getTimestamp() + 500), query, null, new Auths("A,B"));

        assertEquals(2, Iterables.size(actualEvent1));

        assertEquals(2, Iterables.get(actualEvent1, 0).size());
        assertEquals(2, Iterables.get(actualEvent1, 1).size());
    }


    @Test
    public void testExpirationOfAttributes_get() throws Exception {

        Map<String, String> shouldntSee = new MetadataBuilder().setExpiration(1).build();

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis()-500)
                .attr("key1", "val1", meta)
                .attr("key2", "val2", shouldntSee)
                .build();


        store.save(asList(event));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard");


        List<EventIdentifier> eventIndexes = Arrays.asList(new EventIdentifier(event.getId(), event.getTimestamp()));

        Iterable<Event> events = store.get(eventIndexes, null, DEFAULT_AUTHS);


        assertEquals(1, Iterables.size(events));
        assertEquals(1, Iterables.get(events, 0).size());
    }

    @Test
    public void testExpirationOfAttributes_query() {

        Map<String, String> shouldntSee = new MetadataBuilder().setExpiration(1).build();

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis()-500)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", shouldntSee))
                .build();

        store.save(asList(event));

        Node node = QueryBuilder.create().eq("key1", "val1").build();

        Iterable<Event> events = store.query(
                new Date(currentTimeMillis()-5000), new Date(currentTimeMillis()), node, null, DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(events));
        assertEquals(1, Iterables.get(events, 0).size());
    }

    @Test
    public void testQueryKeyNotInIndex() {

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        Node query = QueryBuilder.create().and().eq("key4", "val5").end().build();

        Iterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS);

        assertEquals(0, Iterables.size(itr));
    }


    @Test
    public void testQueryRangeNotInIndex() {

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        Node query = QueryBuilder.create().and().range("key4", 0, 5).end().build();

        Iterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS);

        assertEquals(0, Iterables.size(itr));
    }



    @Test
    public void testGreaterThan() throws Exception {

        long time = currentTimeMillis();
        Event event = EventBuilder.create("", UUID.randomUUID().toString(), time)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key-@#$%^&*()1", 1, meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), time)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key-@#$%^&*()1", 10, meta))
                .build();

        store.save(asList(event, event2));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard", DEFAULT_AUTHS.getAuths());

        CloseableIterable<Event> actualEvent = store.query(new Date(time-50), new Date(time+50), QueryBuilder.create().greaterThan("key-@#$%^&*()1", 9).build(), null, DEFAULT_AUTHS);

        assertEquals(1, size(actualEvent));
        Event actual = actualEvent.iterator().next();
        assertEquals(new HashSet(event2.getAttributes()), new HashSet(actual.getAttributes()));
        assertEquals(actual.getId(), event2.getId());

        actualEvent = store.query(new Date(time), new Date(time), QueryBuilder.create().greaterThan("key-@#$%^&*()1", 0).build(), null, DEFAULT_AUTHS);


        assertEquals(2, size(actualEvent));

    }

    @Test
    public void test_TimeLimit() throws Exception {

        long currentTime = currentTimeMillis();

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTime)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTime - 5000)
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        Node node = QueryBuilder.create().eq("key1", "val1").build();

        CloseableIterable<Event> actualEvent = store.query(new Date(currentTime - 5001), new Date(currentTime + 500), node, null, DEFAULT_AUTHS);
        assertEquals(2, size(actualEvent));

        actualEvent = store.query(new Date(currentTime - 3000), new Date(currentTime+50), node, null, DEFAULT_AUTHS);
        assertEquals(1, size(actualEvent));
    }

    @Test
    public void testGet_withSelection() throws Exception {

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        CloseableIterable<Event> actualEvent = store.get(singletonList(new EventIdentifier(event.getId(), event.getTimestamp())),
                Collections.singleton("key1"), DEFAULT_AUTHS);

        assertEquals(1, size(actualEvent));
        assertNull(actualEvent.iterator().next().get("key2"));
        assertNotNull(actualEvent.iterator().next().get("key1"));
    }


    @Test
    public void testQuery_withSelection() throws Exception {

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        Node query = QueryBuilder.create().and().eq("key1", "val1").eq("key2", "val2").end().build();

        Iterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, Collections.singleton("key1"), DEFAULT_AUTHS);

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

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("`key`.`1`", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("`key`.`1`", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        store.save(asList(event, event2));

        Node query = QueryBuilder.create().and().eq("`key`.`1`", "val1").eq("key2", "val2").end().build();

        Iterator<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS).iterator();

        Event actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(event.getAttributes()));
        } else {
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(event2.getAttributes()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(event.getAttributes()));
        } else {
            assertEquals(new HashSet(actualEvent.getAttributes()), new HashSet(event2.getAttributes()));
        }
    }

    @Test
    public void testQuery_OrQuery() throws Exception {


        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .attr(new Attribute("`key2`", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val2", meta))
                .attr(new Attribute("key3", "val3", meta))
                .build();

        store.save(asList(event, event2));

        AccumuloTestUtils.dumpTable(connector, "eventStore_index");
        AccumuloTestUtils.dumpTable(connector, "eventStore_shard");


        Node query = QueryBuilder.create().or().eq("key3", "val3").eq("`key2`", "val2").end().build();

        Iterator<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS).iterator();

        Event actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getAttributes()), new HashSet(actualEvent.getAttributes()));
        } else {
            assertEquals(new HashSet(event2.getAttributes()), new HashSet(actualEvent.getAttributes()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getAttributes()), new HashSet(actualEvent.getAttributes()));
        } else {
            assertEquals(new HashSet(event2.getAttributes()), new HashSet(actualEvent.getAttributes()));
        }
        assertFalse(itr.hasNext());



        query = QueryBuilder.create().or().eq("key1", "val1").eq("`key1`", "val2").end().build();

        itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS).iterator();

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getAttributes()), new HashSet(actualEvent.getAttributes()));
        } else {
            assertEquals(new HashSet(event2.getAttributes()), new HashSet(actualEvent.getAttributes()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getAttributes()), new HashSet(actualEvent.getAttributes()));
        } else {
            assertEquals(new HashSet(event2.getAttributes()), new HashSet(actualEvent.getAttributes()));
        }
        assertFalse(itr.hasNext());
    }

    @Test
    public void testQuery_SingleEqualsQuery() throws Exception, AccumuloException, AccumuloSecurityException {

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key-1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key-1", "val1", meta))
                .attr(new Attribute("key3", "val3", meta))
                .build();

        store.save(asList(event, event2));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard", DEFAULT_AUTHS.getAuths());

        Node query = QueryBuilder.create().eq("key-1", "val1").build();

        Iterator<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS).iterator();

        Event actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getAttributes()), new HashSet(actualEvent.getAttributes()));
        } else {
            assertEquals(new HashSet(event2.getAttributes()), new HashSet(actualEvent.getAttributes()));
        }

        actualEvent = itr.next();
        if (actualEvent.getId().equals(event.getId())) {
            assertEquals(new HashSet(event.getAttributes()), new HashSet(actualEvent.getAttributes()));
        } else {
            assertEquals(new HashSet(event2.getAttributes()), new HashSet(actualEvent.getAttributes()));
        }
    }


    @Test
    public void testQuery_multipleTypes() throws Exception {

        Event event = EventBuilder.create("type1", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key-@#$%^&*()1", "val1", meta))
                .attr(new Attribute("key2", "val2", meta))
                .build();

        Event event2 = EventBuilder.create("type2", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key-@#$%^&*()1", "val1", meta))
                .attr(new Attribute("key3", "val3", meta))
                .build();

        store.save(asList(event, event2));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard", DEFAULT_AUTHS.getAuths());

        Node query = QueryBuilder.create().eq("key-@#$%^&*()1", "val1").build();

        CloseableIterable<Event> events = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), Sets.newHashSet("type1", "type2"), query, null, DEFAULT_AUTHS);

        assertEquals(2, Iterables.size(events));

        events = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), Sets.newHashSet("type1"), query, null, DEFAULT_AUTHS);

        assertEquals(1, Iterables.size(events));
        assertEquals("type1", Iterables.get(events, 0).getType());

    }

    @Test
    public void testQuery_emptyNodeReturnsNoResults() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Node query = QueryBuilder.create().and().end().build();

        CloseableIterable<Event> itr = store.query(new Date(currentTimeMillis() - 5000),
                new Date(), query, null, DEFAULT_AUTHS);

        assertEquals(0, size(itr));
    }

    @Test
    public void testQuery_MultipleNotInQuery() throws Exception {

        Event event = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("hasIp", "true", meta))
                .attr(new Attribute("ip", "1.1.1.1", meta))
                .build();

        Event event2 = EventBuilder.create("", UUID.randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("hasIp", "true", meta))
                .attr(new Attribute("ip", "2.2.2.2", meta))
                .build();

        Event event3 = EventBuilder.create("", UUID.randomUUID().toString(),
                currentTimeMillis())
                .attr(new Attribute("hasIp", "true", meta))
                .attr(new Attribute("ip", "3.3.3.3", meta))
                .build();

        store.save(asList(event, event2, event3));
        store.flush();

        AccumuloTestUtils.dumpTable(connector, "eventStore_shard");
        AccumuloTestUtils.dumpTable(connector, "eventStore_index");

        Node query = QueryBuilder.create()
                .and()
                .notEq("ip", "1.1.1.1")
                .notEq("ip", "2.2.2.2")
                .notEq("ip", "4.4.4.4")
                .eq("hasIp", "true")
                .end().build();

        Iterator<Event> itr = store.query(
                new Date(currentTimeMillis() - 5000), new Date(), query,
                null, DEFAULT_AUTHS).iterator();

        int x = 0;

        while (itr.hasNext()) {
            x++;
            Event e = itr.next();
            assertEquals("3.3.3.3", e.get("ip").getValue());

        }
        assertEquals(1, x);
    }

    @Test
    public void testQuery_has() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Event event = EventBuilder.create("", "id", currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .build();

        store.save(asList(event));

        Node node = QueryBuilder.create().has("key1").build();

        Iterable<Event> itr = store.query(new Date(0), new Date(), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
        assertEquals(event, Iterables.get(itr, 0));
    }

    @Ignore
    @Test
    public void testQuery_hasNot() throws Exception {

        Event event = EventBuilder.create("", "id", currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .build();

        store.save(asList(event));
        store.flush();

        Node node = QueryBuilder.create().hasNot("key2").build();

        Iterable<Event> itr = store.query(new Date(0), new Date(), node, null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
        assertEquals(event, Iterables.get(itr, 0));
    }


    @Test
    public void testGetAllByType() throws Exception {

        Event event = EventBuilder.create("type-a", "id", currentTimeMillis())
                .attr(new Attribute("key1", "val1", meta))
                .build();

        store.save(asList(event));
        store.flush();

        Iterable<Event> itr = store.getAllByType(new Date(currentTimeMillis() - 50000), new Date(currentTimeMillis()), Sets.newHashSet("type-a"),
                null, DEFAULT_AUTHS);
        assertEquals(1, Iterables.size(itr));
        assertEquals(event, Iterables.get(itr, 0));
    }

}
