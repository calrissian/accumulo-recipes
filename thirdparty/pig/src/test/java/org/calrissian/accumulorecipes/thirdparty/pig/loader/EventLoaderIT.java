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
package org.calrissian.accumulorecipes.thirdparty.pig.loader;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.calrissian.accumulorecipes.test.MockRecordReader;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class EventLoaderIT extends AccumuloInputFormat {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    Event event;
    Job job;

    @Before
    public void setup() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        accumuloMiniClusterDriver.deleteAllTables();
        job = new Job();
    }

    @Test
    public void testGetNext() throws Exception {
        setUpJob();

        List<Pair<String, EventWritable>> mocks = new ArrayList<Pair<String, EventWritable>>();
        mocks.add(new Pair<String, EventWritable>("", new EventWritable(event)));

        MockRecordReader<String, EventWritable> mockRecordReader = new MockRecordReader<String, EventWritable>(mocks);

        EventLoader loader = new EventLoader("q.eq('key','val')");
        loader.prepareToRead(mockRecordReader, new PigSplit());

        org.apache.pig.data.Tuple t;
        int count = 0;

        Iterator<org.calrissian.mango.domain.Attribute> attributes = event.getAttributes().iterator();
        while((t = loader.getNext()) != null) {
            org.calrissian.mango.domain.Attribute attribute = attributes.next();
            count++;
            if(count == 1) {
                assertEquals(event.getId(), t.get(0));
                assertEquals(event.getTimestamp(), t.get(1));
                assertEquals(attribute.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(attribute.getValue()), t.get(3));
                assertEquals(loader.registry.encode(attribute.getValue()), t.get(4));
            } else if(count == 2) {
                assertEquals(event.getId(), t.get(0));
                assertEquals(event.getTimestamp(), t.get(1));
                assertEquals(attribute.getKey(), t.get(2));
                assertEquals(loader.registry.getAlias(attribute.getValue()), t.get(3));
                assertEquals(loader.registry.encode(attribute.getValue()), t.get(4));
            }
        }

        assertEquals(2, count);
    }

    @Test
    public void testSetLocation() throws URISyntaxException, IOException, InterruptedException, AccumuloSecurityException, AccumuloException, TableExistsException {

        String zk = accumuloMiniClusterDriver.getZooKeepers();
        String inst = accumuloMiniClusterDriver.getInstanceName();

        Connector conn = accumuloMiniClusterDriver.getConnector();

        conn.tableOperations().create("eventStore_index");
        conn.tableOperations().create("eventStore_shard");
        Job job = new Job();
        URI location = new URI("event://eventStore_index/eventStore_shard?user=root&pass="+accumuloMiniClusterDriver.getRootPassword()+"&inst=" +
                inst + "&zk=" + zk  +
                "&start=2014-01-01&end=2014-01-02&auths=&types=type1,type2");
        EventLoader loader = new EventLoader("q.eq('key','val')");
        loader.setLocation(location.toString(), job);
        loader.setLocation(location.toString(), job);   // make sure two calls to set location don't fail


        assertEquals(true, isConnectorInfoSet(job));
        assertEquals("root", getPrincipal(job));
        assertEquals(new PasswordToken(accumuloMiniClusterDriver.getRootPassword()), getAuthenticationToken(job));
        assertEquals(zk, getInstance(job).getZooKeepers());
        assertEquals(inst, getInstance(job).getInstanceName());
        assertEquals("eventStore_shard", getInputTableName(job));

    }

    private void setUpJob() throws Exception {
        Connector connector = accumuloMiniClusterDriver.getConnector();
        AccumuloEventStore store = new AccumuloEventStore(connector);
        event = EventBuilder.create("", randomUUID().toString(), currentTimeMillis())
                .attr(new Attribute("key1", "val1"))
                .attr(new Attribute("key2", false))
                .build();
        store.save(singleton(event));
        store.flush();

        EventInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
        EventInputFormat.setZooKeeperInstance(job, accumuloMiniClusterDriver.getClientConfiguration());
        EventInputFormat.setQueryInfo(job, new Date(currentTimeMillis() - 50000), new Date(), Collections.singleton(""),
                QueryBuilder.create().eq("key1", "val1").build());

    }
}
