/*
 * Copyright (C) 2014 The Calrissian Authors
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
package org.calrissian.accumulorecipes.eventstore.support;

import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_BUILDER;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_STORE_CONFIG;
import static org.calrissian.accumulorecipes.test.AccumuloTestUtils.dumpTable;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;
import java.util.Arrays;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.commons.support.qfd.KeyValueIndex;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.junit.Test;

public class EventKeyValueIndexTest {
    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);

        KeyValueIndex eventKeyValueIndex = new KeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = EventBuilder.create("", "id", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2")).build();

        Event event2 = EventBuilder.create("", "id2", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .attr(new Attribute("key3", true))
            .attr(new Attribute("aKey", 1)).build();

        eventStore.save(Arrays.asList(new Event[] {event, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        assertEquals(4, Iterables.size(eventKeyValueIndex.uniqueKeys("", "", Auths.EMPTY)));

        assertEquals("aKey", Iterables.<Pair<String,String>>get(eventKeyValueIndex.uniqueKeys("", "", Auths.EMPTY), 0).getOne());
        assertEquals("key1", Iterables.<Pair<String,String>>get(eventKeyValueIndex.uniqueKeys("", "", Auths.EMPTY), 1).getOne());
        assertEquals("key2", Iterables.<Pair<String,String>>get(eventKeyValueIndex.uniqueKeys("", "", Auths.EMPTY), 2).getOne());
        assertEquals("key3", Iterables.<Pair<String,String>>get(eventKeyValueIndex.uniqueKeys("", "", Auths.EMPTY), 3).getOne());
    }

    @Test
    public void testTypes() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);


        KeyValueIndex eventKeyValueIndex = new KeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = EventBuilder.create("type1", "id", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2")).build();

        Event event2 = EventBuilder.create("type2", "id2", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .attr(new Attribute("key3", true))
            .attr(new Attribute("aKey", 1)).build();


        eventStore.save(Arrays.asList(new Event[] {event, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        CloseableIterable<String> types = eventKeyValueIndex.getTypes("", Auths.EMPTY);

        assertEquals(2, Iterables.size(types));

        assertEquals("type1", Iterables.get(types, 0));
        assertEquals("type2", Iterables.get(types, 1));
    }

    @Test
    public void testTypesWithPrefix() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);


        KeyValueIndex eventKeyValueIndex = new KeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = EventBuilder.create("type1", "id", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2")).build();

        Event event2 = EventBuilder.create("type2", "id2", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .attr(new Attribute("key3", true))
            .attr(new Attribute("aKey", 1)).build();

        Event event3 = EventBuilder.create("aType3", "id2", System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .attr(new Attribute("key3", true))
            .attr(new Attribute("aKey", 1)).build();


        eventStore.save(Arrays.asList(new Event[] {event, event2, event3}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        CloseableIterable<String> types = eventKeyValueIndex.getTypes("t", Auths.EMPTY);

        assertEquals(2, Iterables.size(types));

        assertEquals("type1", Iterables.get(types, 0));
        assertEquals("type2", Iterables.get(types, 1));
    }


    @Test
    public void testUniqueValues() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);


        KeyValueIndex eventKeyValueIndex = new KeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = EventBuilder.create("type1", "id")
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2")).build();

        Event event1 = EventBuilder.create("type1", "id")
            .attr(new Attribute("key1", "theVal1"))
            .attr(new Attribute("key2", "aVal")).build();

        Event event2 = EventBuilder.create("type2", "id2")
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", "val2"))
            .attr(new Attribute("key3", true))
            .attr(new Attribute("aKey", 1)).build();

        eventStore.save(Arrays.asList(new Event[] {event, event1, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        /**
         * Test with prefix which contains value
         */
        CloseableIterable<Object> types = eventKeyValueIndex.uniqueValuesForKey("v", "type1", "string", "key1", Auths.EMPTY);
        assertEquals(1, Iterables.size(types));
        assertEquals("val1", Iterables.get(types, 0));

        /**
         * Test with prefix that does not contain value
         */
        types = eventKeyValueIndex.uniqueValuesForKey("a", "type1", "string", "key1", Auths.EMPTY);
        assertEquals(0, Iterables.size(types));

        /**
         * Test with no prefix
         */
        types = eventKeyValueIndex.uniqueValuesForKey("", "type1", "string", "key2", Auths.EMPTY);
        assertEquals(2, Iterables.size(types));
        assertEquals("aVal", Iterables.get(types, 0));
        assertEquals("val2", Iterables.get(types, 1));
    }

}
