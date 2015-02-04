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
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Test;

public class EventKeyValueIndexTest {
    @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);


        EventKeyValueIndex eventKeyValueIndex = new EventKeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = new BaseEvent("id");
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent("id2");
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));
        event2.put(new Tuple("key3", true));
        event2.put(new Tuple("aKey", 1));

        eventStore.save(Arrays.asList(new Event[] {event, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        assertEquals(4, Iterables.size(eventKeyValueIndex.uniqueKeys("", "", new Auths())));

        assertEquals("aKey", Iterables.get(eventKeyValueIndex.uniqueKeys("", "", new Auths()), 0).getOne());
        assertEquals("key1", Iterables.get(eventKeyValueIndex.uniqueKeys("", "", new Auths()), 1).getOne());
        assertEquals("key2", Iterables.get(eventKeyValueIndex.uniqueKeys("", "", new Auths()), 2).getOne());
        assertEquals("key3", Iterables.get(eventKeyValueIndex.uniqueKeys("", "", new Auths()), 3).getOne());
    }

    @Test
    public void testTypes() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);


        EventKeyValueIndex eventKeyValueIndex = new EventKeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = new BaseEvent("type1", "id");
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event2 = new BaseEvent("type2", "id2");
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));
        event2.put(new Tuple("key3", true));
        event2.put(new Tuple("aKey", 1));

        eventStore.save(Arrays.asList(new Event[] {event, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        CloseableIterable<String> types = eventKeyValueIndex.getTypes(new Auths());

        assertEquals(2, Iterables.size(types));

        assertEquals("type1", Iterables.get(types, 0));
        assertEquals("type2", Iterables.get(types, 1));
    }

    @Test
    public void testUniqueValues() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());
        EventStore eventStore = new AccumuloEventStore(connector);


        EventKeyValueIndex eventKeyValueIndex = new EventKeyValueIndex(
            connector, "eventStore_index", DEFAULT_SHARD_BUILDER, DEFAULT_STORE_CONFIG,
            LEXI_TYPES
        );

        Event event = new BaseEvent("type1", "id");
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));

        Event event1 = new BaseEvent("type1", "id");
        event.put(new Tuple("key1", "theVal1"));
        event.put(new Tuple("key2", "aVal"));

        Event event2 = new BaseEvent("type2", "id2");
        event2.put(new Tuple("key1", "val1"));
        event2.put(new Tuple("key2", "val2"));
        event2.put(new Tuple("key3", true));
        event2.put(new Tuple("aKey", 1));

        eventStore.save(Arrays.asList(new Event[] {event, event1, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        /**
         * Test with prefix which contains value
         */
        CloseableIterable<Object> types = eventKeyValueIndex.uniqueValuesForKey("v", "type1", "string", "key1", new Auths());
        assertEquals(1, Iterables.size(types));
        assertEquals("val1", Iterables.get(types, 0));

        /**
         * Test with prefix that does not contain value
         */
        types = eventKeyValueIndex.uniqueValuesForKey("a", "type1", "string", "key1", new Auths());
        assertEquals(0, Iterables.size(types));

        /**
         * Test with no prefix
         */
        types = eventKeyValueIndex.uniqueValuesForKey("", "type1", "string", "key2", new Auths());
        assertEquals(2, Iterables.size(types));
        assertEquals("aVal", Iterables.get(types, 0));
        assertEquals("val2", Iterables.get(types, 1));
    }

}
