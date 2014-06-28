package org.calrissian.accumulorecipes.eventstore.support;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Test;

import java.util.Arrays;

import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.*;
import static org.calrissian.accumulorecipes.test.AccumuloTestUtils.dumpTable;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;

public class EventKeyValueIndexTest {
     @Test
    public void test() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {

        Instance instance = new MockInstance();
        Connector connector = instance.getConnector("root", "".getBytes());

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

        eventKeyValueIndex.indexKeyValues(Arrays.asList(new Event[] {event, event2}));

        dumpTable(connector, DEFAULT_IDX_TABLE_NAME);

        assertEquals(4, Iterables.size(eventKeyValueIndex.uniqueKeys("", new Auths())));


    }

}
