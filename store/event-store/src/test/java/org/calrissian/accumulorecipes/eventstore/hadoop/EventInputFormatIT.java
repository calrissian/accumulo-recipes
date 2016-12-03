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
package org.calrissian.accumulorecipes.eventstore.hadoop;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.calrissian.accumulorecipes.commons.hadoop.EventWritable;
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore;
import org.calrissian.accumulorecipes.test.AccumuloMiniClusterDriver;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Attribute;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.event.EventBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class EventInputFormatIT {

    @ClassRule
    public static AccumuloMiniClusterDriver accumuloMiniClusterDriver = new AccumuloMiniClusterDriver();

    public static Event event;

    @Before
    public void setup() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
      accumuloMiniClusterDriver.deleteAllTables();
      event = null;
      TestMapper.entry = null;
    }

    @Test
    public void test() throws Exception {

        Connector connector = accumuloMiniClusterDriver.getConnector();
        AccumuloEventStore store = new AccumuloEventStore(connector);
        event = EventBuilder.create("", UUID.randomUUID().toString(), System.currentTimeMillis())
            .attr(new Attribute("key1", "val1"))
            .attr(new Attribute("key2", false)).build();
        store.save(singleton(event));
        store.flush();

        Job job = new Job(new Configuration());
        job.setJarByClass(getClass());
        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(EventInputFormat.class);
        EventInputFormat.setZooKeeperInstance(job, accumuloMiniClusterDriver.getClientConfiguration());
        EventInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
        EventInputFormat.setQueryInfo(job, new Date(System.currentTimeMillis() - 50000), new Date(), Collections.singleton(""),
                QueryBuilder.create().eq("key1", "val1").build());
        job.setOutputFormatClass(NullOutputFormat.class);

        job.submit();
        job.waitForCompletion(true);

        assertNotNull(TestMapper.entry);
        assertEquals(TestMapper.entry.getId(), event.getId());
        assertTrue(TestMapper.entry.getTimestamp() - event.getTimestamp() < 50);
        assertEquals(new HashSet<Attribute>(TestMapper.entry.getAttributes()), new HashSet<Attribute>(event.getAttributes()));

    }

  @Test
  public void testNoQuery() throws Exception {

    Connector connector = accumuloMiniClusterDriver.getConnector();
    AccumuloEventStore store = new AccumuloEventStore(connector);
    event = EventBuilder.create("", UUID.randomUUID().toString(), System.currentTimeMillis())
        .attr(new Attribute("key1", "val1"))
        .attr(new Attribute("key2", false)).build();
    store.save(singleton(event));
    store.flush();
      AccumuloTestUtils.dumpTable(connector, "eventStore_shard");
    Job job = new Job(new Configuration());
    job.setJarByClass(getClass());
    job.setMapperClass(TestMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(EventInputFormat.class);
      EventInputFormat.setZooKeeperInstance(job, accumuloMiniClusterDriver.getClientConfiguration());
      EventInputFormat.setInputInfo(job, "root", accumuloMiniClusterDriver.getRootPassword().getBytes(), new Authorizations());
    EventInputFormat.setQueryInfo(job, new Date(System.currentTimeMillis() - 50000), new Date(), Collections.singleton(""));
    job.setOutputFormatClass(NullOutputFormat.class);

    job.submit();
    job.waitForCompletion(true);

    System.out.println("RESULT: " + TestMapper.entry);

    assertNotNull(TestMapper.entry);
    assertEquals(TestMapper.entry.getId(), event.getId());
    assertEquals(new HashSet<Attribute>(TestMapper.entry.getAttributes()), new HashSet<Attribute>(event.getAttributes()));

  }

    public static class TestMapper extends Mapper<Key, EventWritable, Text, Text> {

        public static Event entry;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(Key key, EventWritable value, Context context) throws IOException, InterruptedException {
            entry = value.get();
        }
    }
}
