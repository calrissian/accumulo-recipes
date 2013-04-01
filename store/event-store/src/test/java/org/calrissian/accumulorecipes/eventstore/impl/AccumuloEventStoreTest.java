package org.calrissian.accumulorecipes.eventstore.impl;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.accumulorecipes.eventstore.domain.Event;
import org.calrissian.accumulorecipes.eventstore.support.EventIterator;
import org.calrissian.commons.domain.Tuple;
import org.calrissian.commons.serialization.ObjectMapperContext;
import org.calrissian.mango.types.TypeContext;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AccumuloEventStoreTest {

    @Test
    public void test() throws Exception, AccumuloException, AccumuloSecurityException {

        Event event = new Event(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1", ""));
        event.put(new Tuple("key2", "val2", ""));

        MockInstance instance = new MockInstance();
        Connector conn = instance.getConnector("username", "password".getBytes());

        AccumuloEventStore store = new AccumuloEventStore(conn);
        store.put(Collections.singleton(event));

        System.out.println(ObjectMapperContext.getInstance().getObjectMapper().writeValueAsString(event));





        Scanner scanner = conn.createScanner("eventStore_shard", new Authorizations());

        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("key1", "val1");
        fields.put("key2", "val2");


        IteratorSetting intersectingIterator = new IteratorSetting(16, "ii", EventIterator.class);
        Text[] columns = new Text[2];

        columns[0] = new Text(AccumuloEventStore.SHARD_PREFIX_B + AccumuloEventStore.DELIM + "key1" +
                              AccumuloEventStore.DELIM + TypeContext.getInstance().getAliasForType("val1") +
                              AccumuloEventStore.DELIM + "val1");

        columns[1] = new Text(AccumuloEventStore.SHARD_PREFIX_B + AccumuloEventStore.DELIM + "key2" +
                AccumuloEventStore.DELIM + TypeContext.getInstance().getAliasForType("val2") +
                AccumuloEventStore.DELIM + "val2");

        IntersectingIterator.setColumnFamilies(intersectingIterator, columns);

        scanner.addScanIterator(intersectingIterator);
        for(Map.Entry<Key,Value> entry : scanner) {

            System.out.println("DOC: " + new String(entry.getValue().get()));
        }

        Scanner scanner2 = conn.createScanner("eventStore_index", new Authorizations());
        for(Map.Entry<Key, Value> entry : scanner2) {
            System.out.println(entry);
        }
    }
}
